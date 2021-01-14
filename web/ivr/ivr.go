package ivr

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/nyaruka/gocommon/httpx"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/mailroom/config"
	"github.com/nyaruka/mailroom/core/ivr"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/tasks/handler"
	"github.com/nyaruka/mailroom/web"

	"github.com/go-chi/chi"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func init() {
	web.RegisterRoute(http.MethodPost, "/mr/ivr/c/{uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}}/handle", newIVRHandler(handleFlow))
	web.RegisterRoute(http.MethodPost, "/mr/ivr/c/{uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}}/status", newIVRHandler(handleStatus))
	web.RegisterRoute(http.MethodPost, "/mr/ivr/c/{uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}}/incoming", newIVRHandler(handleIncomingCall))
}

type ivrHandlerFn func(ctx context.Context, s *web.Server, r *http.Request, w http.ResponseWriter) (*models.Channel, *models.ChannelConnection, error)

func newIVRHandler(handler ivrHandlerFn) web.Handler {
	return func(ctx context.Context, s *web.Server, r *http.Request, w http.ResponseWriter) error {
		recorder := httpx.NewRecorder(r, w)
		ww := recorder.ResponseWriter

		channel, connection, rerr := handler(ctx, s, r, ww)

		if channel != nil {
			trace, err := recorder.End()
			if err != nil {
				logrus.WithError(err).WithField("http_request", r).Error("error recording IVR request")
			}

			desc := "IVR event handled"
			isError := false
			if trace.Response == nil || trace.Response.StatusCode != http.StatusOK {
				desc = "IVR Error"
				isError = true
			}

			log := models.NewChannelLog(trace, isError, desc, channel, connection)
			err = models.InsertChannelLogs(ctx, s.DB, []*models.ChannelLog{log})
			if err != nil {
				logrus.WithError(err).WithField("http_request", r).Error("error writing ivr channel log")
			}
		}

		return rerr
	}
}

func handleIncomingCall(ctx context.Context, s *web.Server, r *http.Request, w http.ResponseWriter) (*models.Channel, *models.ChannelConnection, error) {
	channelUUID := assets.ChannelUUID(chi.URLParam(r, "uuid"))

	// load the org id for this UUID (we could load the entire channel here but we want to take the same paths through everything else)
	orgID, err := models.OrgIDForChannelUUID(ctx, s.DB, channelUUID)
	if err != nil {
		return nil, nil, writeClientError(w, err)
	}

	// load our org assets
	oa, err := models.GetOrgAssets(ctx, s.DB, orgID)
	if err != nil {
		return nil, nil, writeClientError(w, errors.Wrapf(err, "error loading org assets"))
	}

	// and our channel
	channel := oa.ChannelByUUID(channelUUID)
	if channel == nil {
		return nil, nil, writeClientError(w, errors.Wrapf(err, "no active channel with uuid: %s", channelUUID))
	}

	// get the right kind of client
	client, err := ivr.GetClient(channel)
	if client == nil {
		return channel, nil, writeClientError(w, errors.Wrapf(err, "unable to load client for channel: %s", channelUUID))
	}

	// validate this request's signature
	err = client.ValidateRequestSignature(r)
	if err != nil {
		return channel, nil, client.WriteErrorResponse(w, errors.Wrapf(err, "request failed signature validation"))
	}

	// lookup the URN of the caller
	urn, err := client.URNForRequest(r)
	if err != nil {
		return channel, nil, client.WriteErrorResponse(w, errors.Wrapf(err, "unable to find URN in request"))
	}

	// get the contact for this URN
	contact, _, _, err := models.GetOrCreateContact(ctx, s.DB, oa, []urns.URN{urn}, channel.ID())
	if err != nil {
		return channel, nil, client.WriteErrorResponse(w, errors.Wrapf(err, "unable to get contact by urn"))
	}

	urn, err = models.URNForURN(ctx, s.DB, oa, urn)
	if err != nil {
		return channel, nil, client.WriteErrorResponse(w, errors.Wrapf(err, "unable to load urn"))
	}

	// urn ID
	urnID := models.GetURNID(urn)
	if urnID == models.NilURNID {
		return channel, nil, client.WriteErrorResponse(w, errors.Wrapf(err, "unable to get id for URN"))
	}

	// we first create an incoming call channel event and see if that matches
	event := models.NewChannelEvent(models.MOCallEventType, oa.OrgID(), channel.ID(), contact.ID(), urnID, nil, false)

	externalID, err := client.CallIDForRequest(r)
	if err != nil {
		return channel, nil, client.WriteErrorResponse(w, errors.Wrapf(err, "unable to get external id from request"))
	}

	// create our connection
	conn, err := models.InsertIVRConnection(
		ctx, s.DB, oa.OrgID(), channel.ID(), models.NilStartID, contact.ID(), urnID,
		models.ConnectionDirectionIn, models.ConnectionStatusInProgress, externalID,
	)
	if err != nil {
		return channel, nil, client.WriteErrorResponse(w, errors.Wrapf(err, "error creating ivr connection"))
	}

	// try to handle this event
	session, err := handler.HandleChannelEvent(ctx, s.DB, s.RP, models.MOCallEventType, event, conn)
	if err != nil {
		logrus.WithError(err).WithField("http_request", r).Error("error handling incoming call")

		return channel, conn, client.WriteErrorResponse(w, errors.Wrapf(err, "error handling incoming call"))
	}

	// we got a session back so we have an active call trigger
	if session != nil {
		// build our resume URL
		resumeURL := buildResumeURL(channel, conn, urn)

		// have our client output our session status
		err = client.WriteSessionResponse(session, urn, resumeURL, r, w)
		if err != nil {
			return channel, conn, errors.Wrapf(err, "error writing ivr response for start")
		}

		return channel, conn, nil
	}

	// no session means no trigger, create a missed call event instead
	// we first create an incoming call channel event and see if that matches
	event = models.NewChannelEvent(models.MOMissEventType, oa.OrgID(), channel.ID(), contact.ID(), urnID, nil, false)
	err = event.Insert(ctx, s.DB)
	if err != nil {
		return channel, conn, client.WriteErrorResponse(w, errors.Wrapf(err, "error inserting channel event"))
	}

	// try to handle it, this time looking for a missed call event
	session, err = handler.HandleChannelEvent(ctx, s.DB, s.RP, models.MOMissEventType, event, nil)
	if err != nil {
		logrus.WithError(err).WithField("http_request", r).Error("error handling missed call")
		return channel, conn, client.WriteErrorResponse(w, errors.Wrapf(err, "error handling missed call"))
	}

	// write our empty response
	return channel, conn, client.WriteEmptyResponse(w, "missed call handled")
}

const (
	actionStart  = "start"
	actionResume = "resume"
	actionStatus = "status"
)

// IVRRequest is our form for what fields we expect in IVR callbacks
type IVRRequest struct {
	ConnectionID models.ConnectionID `form:"connection" validate:"required"`
	Action       string              `form:"action"     validate:"required"`
}

// writeClientError is just a small utility method to write out a simple JSON error when we don't have a client yet
// to do it on our behalf
func writeClientError(w http.ResponseWriter, err error) error {
	w.Header().Set("Content-type", "application/json")
	w.WriteHeader(http.StatusBadRequest)
	response := map[string]string{
		"error": err.Error(),
	}
	serialized, err := json.Marshal(response)
	if err != nil {
		return errors.Wrapf(err, "error serializing error")
	}
	_, err = w.Write([]byte(serialized))
	return errors.Wrapf(err, "error writing error")
}

func buildResumeURL(channel *models.Channel, conn *models.ChannelConnection, urn urns.URN) string {
	domain := channel.ConfigValue(models.ChannelConfigCallbackDomain, config.Mailroom.Domain)
	form := url.Values{
		"action":     []string{actionResume},
		"connection": []string{fmt.Sprintf("%d", conn.ID())},
		"urn":        []string{urn.String()},
	}

	return fmt.Sprintf("https://%s/mr/ivr/c/%s/handle?%s", domain, channel.UUID(), form.Encode())
}

// handleFlow handles all incoming IVR requests related to a flow (status is handled elsewhere)
func handleFlow(ctx context.Context, s *web.Server, r *http.Request, w http.ResponseWriter) (*models.Channel, *models.ChannelConnection, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second*55)
	defer cancel()

	request := &IVRRequest{}
	if err := web.DecodeAndValidateForm(request, r); err != nil {
		return nil, nil, errors.Wrapf(err, "request failed validation")
	}

	// load our connection
	conn, err := models.SelectChannelConnection(ctx, s.DB, request.ConnectionID)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "unable to load channel connection with id: %d", request.ConnectionID)
	}

	// load our org assets
	oa, err := models.GetOrgAssets(ctx, s.DB, conn.OrgID())
	if err != nil {
		return nil, nil, writeClientError(w, errors.Wrapf(err, "error loading org assets"))
	}

	// and our channel
	channel := oa.ChannelByID(conn.ChannelID())
	if channel == nil {
		return nil, nil, writeClientError(w, errors.Errorf("no active channel with id: %d", conn.ChannelID()))
	}

	// get the right kind of client
	client, err := ivr.GetClient(channel)
	if client == nil {
		return channel, conn, writeClientError(w, errors.Wrapf(err, "unable to load client for channel: %d", conn.ChannelID()))
	}

	// validate this request's signature if relevant
	err = client.ValidateRequestSignature(r)
	if err != nil {
		return channel, conn, writeClientError(w, errors.Wrapf(err, "request failed signature validation"))
	}

	// load our contact
	contacts, err := models.LoadContacts(ctx, s.DB, oa, []models.ContactID{conn.ContactID()})
	if err != nil {
		return channel, conn, client.WriteErrorResponse(w, errors.Wrapf(err, "no such contact"))
	}
	if len(contacts) == 0 {
		return channel, conn, client.WriteErrorResponse(w, errors.Errorf("no contact with id: %d", conn.ContactID()))
	}
	if contacts[0].Status() != models.ContactStatusActive {
		return channel, conn, client.WriteErrorResponse(w, errors.Errorf("no contact with id: %d", conn.ContactID()))
	}

	// load the URN for this connection
	urn, err := models.URNForID(ctx, s.DB, oa, conn.ContactURNID())
	if err != nil {
		return channel, conn, client.WriteErrorResponse(w, errors.Errorf("unable to find connection urn: %d", conn.ContactURNID()))
	}

	// make sure our URN is indeed present on our contact, no funny business
	found := false
	for _, u := range contacts[0].URNs() {
		if u.Identity() == urn.Identity() {
			found = true
		}
	}
	if !found {
		return channel, conn, client.WriteErrorResponse(w, errors.Errorf("unable to find URN: %s on contact: %d", urn, conn.ContactID()))
	}

	resumeURL := buildResumeURL(channel, conn, urn)

	// if this a start, start our contact
	switch request.Action {
	case actionStart:
		err = ivr.StartIVRFlow(
			ctx, s.DB, s.RP, client, resumeURL,
			oa, channel, conn, contacts[0], urn, conn.StartID(),
			r, w,
		)

	case actionResume:
		err = ivr.ResumeIVRFlow(
			ctx, s.Config, s.DB, s.RP, s.Storage, resumeURL, client,
			oa, channel, conn, contacts[0], urn,
			r, w,
		)

	case actionStatus:
		err = ivr.HandleIVRStatus(
			ctx, s.DB, s.RP, oa, client, conn,
			r, w,
		)

	default:
		err = client.WriteErrorResponse(w, errors.Errorf("unknown action: %s", request.Action))
	}

	// had an error? mark our connection as errored and log it
	if err != nil {
		logrus.WithError(err).WithField("http_request", r).Error("error while handling IVR")
		return channel, conn, ivr.WriteErrorResponse(ctx, s.DB, client, conn, w, err)
	}

	return channel, conn, nil
}

// handleStatus handles all incoming IVR events / status updates
func handleStatus(ctx context.Context, s *web.Server, r *http.Request, w http.ResponseWriter) (*models.Channel, *models.ChannelConnection, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second*55)
	defer cancel()

	channelUUID := assets.ChannelUUID(chi.URLParam(r, "uuid"))

	// load the org id for this UUID (we could load the entire channel here but we want to take the same paths through everything else)
	orgID, err := models.OrgIDForChannelUUID(ctx, s.DB, channelUUID)
	if err != nil {
		return nil, nil, writeClientError(w, err)
	}

	// load our org assets
	oa, err := models.GetOrgAssets(ctx, s.DB, orgID)
	if err != nil {
		return nil, nil, writeClientError(w, errors.Wrapf(err, "error loading org assets"))
	}

	// and our channel
	channel := oa.ChannelByUUID(channelUUID)
	if channel == nil {
		return nil, nil, writeClientError(w, errors.Wrapf(err, "no active channel with uuid: %s", channelUUID))
	}

	// get the right kind of client
	client, err := ivr.GetClient(channel)
	if client == nil {
		return channel, nil, writeClientError(w, errors.Wrapf(err, "unable to load client for channel: %s", channelUUID))
	}

	// validate this request's signature if relevant
	err = client.ValidateRequestSignature(r)
	if err != nil {
		return channel, nil, writeClientError(w, errors.Wrapf(err, "request failed signature validation"))
	}

	// get our external id
	externalID, err := client.CallIDForRequest(r)
	if err != nil {
		return channel, nil, client.WriteErrorResponse(w, errors.Wrapf(err, "unable to get call id for request"))
	}

	// load our connection
	conn, err := models.SelectChannelConnectionByExternalID(ctx, s.DB, channel.ID(), models.ConnectionTypeIVR, externalID)
	if errors.Cause(err) == sql.ErrNoRows {
		return channel, nil, client.WriteEmptyResponse(w, "unknown connection, ignoring")
	}
	if err != nil {
		return channel, nil, client.WriteErrorResponse(w, errors.Wrapf(err, "unable to load channel connection with id: %s", externalID))
	}

	err = ivr.HandleIVRStatus(ctx, s.DB, s.RP, oa, client, conn, r, w)

	// had an error? mark our connection as errored and log it
	if err != nil {
		logrus.WithError(err).WithField("http_request", r).Error("error while handling status")
		return channel, conn, ivr.WriteErrorResponse(ctx, s.DB, client, conn, w, err)
	}

	return channel, conn, nil
}
