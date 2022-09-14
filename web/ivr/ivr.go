package ivr

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/nyaruka/gocommon/httpx"
	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/mailroom/core/ivr"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/tasks/handler"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/web"

	"github.com/go-chi/chi"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func init() {
	web.RegisterRoute(http.MethodPost, "/mr/ivr/c/{uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}}/handle", newIVRHandler(handleCallback, models.ChannelLogTypeIVRCallback))
	web.RegisterRoute(http.MethodPost, "/mr/ivr/c/{uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}}/status", newIVRHandler(handleStatus, models.ChannelLogTypeIVRStatus))
	web.RegisterRoute(http.MethodPost, "/mr/ivr/c/{uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}}/incoming", newIVRHandler(handleIncoming, models.ChannelLogTypeIVRIncoming))
}

type ivrHandlerFn func(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, ch *models.Channel, svc ivr.Service, r *http.Request, w http.ResponseWriter, clog *models.ChannelLog) (*models.ChannelConnection, error)

func newIVRHandler(handler ivrHandlerFn, logType models.ChannelLogType) web.Handler {
	return func(ctx context.Context, rt *runtime.Runtime, r *http.Request, w http.ResponseWriter) error {
		channelUUID := assets.ChannelUUID(chi.URLParam(r, "uuid"))

		// load the org id for this UUID (we could load the entire channel here but we want to take the same paths through everything else)
		orgID, err := models.OrgIDForChannelUUID(ctx, rt.DB, channelUUID)
		if err != nil {
			return writeGenericErrorResponse(w, err)
		}

		// load our org assets
		oa, err := models.GetOrgAssets(ctx, rt, orgID)
		if err != nil {
			return writeGenericErrorResponse(w, errors.Wrapf(err, "error loading org assets"))
		}

		// and our channel
		ch := oa.ChannelByUUID(channelUUID)
		if ch == nil {
			return writeGenericErrorResponse(w, errors.Wrapf(err, "no active channel with uuid: %s", channelUUID))
		}

		// get the IVR service for this channel
		svc, err := ivr.GetService(ch)
		if svc == nil {
			return writeGenericErrorResponse(w, errors.Wrapf(err, "unable to get service for channel: %s", ch.UUID()))
		}

		// validate this request's signature
		err = svc.ValidateRequestSignature(r)
		if err != nil {
			return svc.WriteErrorResponse(w, errors.Wrapf(err, "request failed signature validation"))
		}

		recorder, err := httpx.NewRecorder(r, w, true)
		if err != nil {
			return errors.Wrapf(err, "error reading request body")
		}

		clog := models.NewChannelLogForIncoming(logType, ch, recorder, svc.RedactValues(ch))

		connection, rerr := handler(ctx, rt, oa, ch, svc, r, recorder.ResponseWriter, clog)
		clog.SetConnection(connection)

		if err := recorder.End(); err != nil {
			logrus.WithError(err).WithField("http_request", r).Error("error recording IVR request")
		}

		clog.End()

		err = models.InsertChannelLogs(ctx, rt.DB, []*models.ChannelLog{clog})
		if err != nil {
			logrus.WithError(err).WithField("http_request", r).Error("error writing ivr channel log")
		}

		return rerr
	}
}

func handleIncoming(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, ch *models.Channel, svc ivr.Service, r *http.Request, w http.ResponseWriter, clog *models.ChannelLog) (*models.ChannelConnection, error) {
	// lookup the URN of the caller
	urn, err := svc.URNForRequest(r)
	if err != nil {
		return nil, svc.WriteErrorResponse(w, errors.Wrapf(err, "unable to find URN in request"))
	}

	// get the contact for this URN
	contact, _, _, err := models.GetOrCreateContact(ctx, rt.DB, oa, []urns.URN{urn}, ch.ID())
	if err != nil {
		return nil, svc.WriteErrorResponse(w, errors.Wrapf(err, "unable to get contact by urn"))
	}

	urn, err = models.URNForURN(ctx, rt.DB, oa, urn)
	if err != nil {
		return nil, svc.WriteErrorResponse(w, errors.Wrapf(err, "unable to load urn"))
	}

	// urn ID
	urnID := models.GetURNID(urn)
	if urnID == models.NilURNID {
		return nil, svc.WriteErrorResponse(w, errors.Wrapf(err, "unable to get id for URN"))
	}

	// we first create an incoming call channel event and see if that matches
	event := models.NewChannelEvent(models.MOCallEventType, oa.OrgID(), ch.ID(), contact.ID(), urnID, nil, false)

	externalID, err := svc.CallIDForRequest(r)
	if err != nil {
		return nil, svc.WriteErrorResponse(w, errors.Wrapf(err, "unable to get external id from request"))
	}

	// create our connection
	conn, err := models.InsertIVRConnection(
		ctx, rt.DB, oa.OrgID(), ch.ID(), models.NilStartID, contact.ID(), urnID,
		models.ConnectionDirectionIn, models.ConnectionStatusInProgress, externalID,
	)
	if err != nil {
		return nil, svc.WriteErrorResponse(w, errors.Wrapf(err, "error creating ivr connection"))
	}

	// try to handle this event
	session, err := handler.HandleChannelEvent(ctx, rt, models.MOCallEventType, event, conn)
	if err != nil {
		logrus.WithError(err).WithField("http_request", r).Error("error handling incoming call")

		return conn, svc.WriteErrorResponse(w, errors.Wrapf(err, "error handling incoming call"))
	}

	// we got a session back so we have an active call trigger
	if session != nil {
		// build our resume URL
		resumeURL := buildResumeURL(rt.Config, ch, conn, urn)

		// have our client output our session status
		err = svc.WriteSessionResponse(ctx, rt, ch, conn, session, urn, resumeURL, r, w)
		if err != nil {
			return conn, errors.Wrapf(err, "error writing ivr response for start")
		}

		return conn, nil
	}

	// no session means no trigger, create a missed call event instead
	// we first create an incoming call channel event and see if that matches
	event = models.NewChannelEvent(models.MOMissEventType, oa.OrgID(), ch.ID(), contact.ID(), urnID, nil, false)
	err = event.Insert(ctx, rt.DB)
	if err != nil {
		return conn, svc.WriteErrorResponse(w, errors.Wrapf(err, "error inserting channel event"))
	}

	// try to handle it, this time looking for a missed call event
	_, err = handler.HandleChannelEvent(ctx, rt, models.MOMissEventType, event, nil)
	if err != nil {
		logrus.WithError(err).WithField("http_request", r).Error("error handling missed call")
		return conn, svc.WriteErrorResponse(w, errors.Wrapf(err, "error handling missed call"))
	}

	// write our empty response
	return conn, svc.WriteEmptyResponse(w, "missed call handled")
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

// writeGenericErrorResponse is just a small utility method to write out a simple JSON error when we don't have a client yet
func writeGenericErrorResponse(w http.ResponseWriter, err error) error {
	w.Header().Set("Content-type", "application/json")
	w.WriteHeader(http.StatusBadRequest)
	_, err = w.Write(jsonx.MustMarshal(map[string]string{"error": err.Error()}))
	return err
}

func buildResumeURL(cfg *runtime.Config, channel *models.Channel, conn *models.ChannelConnection, urn urns.URN) string {
	domain := channel.ConfigValue(models.ChannelConfigCallbackDomain, cfg.Domain)
	form := url.Values{
		"action":     []string{actionResume},
		"connection": []string{fmt.Sprintf("%d", conn.ID())},
		"urn":        []string{urn.String()},
	}

	return fmt.Sprintf("https://%s/mr/ivr/c/%s/handle?%s", domain, channel.UUID(), form.Encode())
}

// handles all incoming IVR requests related to a flow (status is handled elsewhere)
func handleCallback(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, ch *models.Channel, svc ivr.Service, r *http.Request, w http.ResponseWriter, clog *models.ChannelLog) (*models.ChannelConnection, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second*55)
	defer cancel()

	request := &IVRRequest{}
	if err := web.DecodeAndValidateForm(request, r); err != nil {
		return nil, errors.Wrapf(err, "request failed validation")
	}

	// load our connection
	conn, err := models.SelectChannelConnection(ctx, rt.DB, oa.OrgID(), request.ConnectionID)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to load channel connection with id: %d", request.ConnectionID)
	}

	// load our contact
	contacts, err := models.LoadContacts(ctx, rt.ReadonlyDB, oa, []models.ContactID{conn.ContactID()})
	if err != nil {
		return conn, svc.WriteErrorResponse(w, errors.Wrapf(err, "no such contact"))
	}
	if len(contacts) == 0 {
		return conn, svc.WriteErrorResponse(w, errors.Errorf("no contact with id: %d", conn.ContactID()))
	}
	if contacts[0].Status() != models.ContactStatusActive {
		return conn, svc.WriteErrorResponse(w, errors.Errorf("no contact with id: %d", conn.ContactID()))
	}

	// load the URN for this connection
	urn, err := models.URNForID(ctx, rt.DB, oa, conn.ContactURNID())
	if err != nil {
		return conn, svc.WriteErrorResponse(w, errors.Errorf("unable to find connection urn: %d", conn.ContactURNID()))
	}

	// make sure our URN is indeed present on our contact, no funny business
	found := false
	for _, u := range contacts[0].URNs() {
		if u.Identity() == urn.Identity() {
			found = true
		}
	}
	if !found {
		return conn, svc.WriteErrorResponse(w, errors.Errorf("unable to find URN: %s on contact: %d", urn, conn.ContactID()))
	}

	resumeURL := buildResumeURL(rt.Config, ch, conn, urn)

	// if this a start, start our contact
	switch request.Action {
	case actionStart:
		err = ivr.StartIVRFlow(ctx, rt, svc, resumeURL, oa, ch, conn, contacts[0], urn, conn.StartID(), r, w, clog)
	case actionResume:
		err = ivr.ResumeIVRFlow(ctx, rt, resumeURL, svc, oa, ch, conn, contacts[0], urn, r, w, clog)
	case actionStatus:
		err = ivr.HandleIVRStatus(ctx, rt, oa, svc, conn, r, w, clog)

	default:
		err = svc.WriteErrorResponse(w, errors.Errorf("unknown action: %s", request.Action))
	}

	// had an error? mark our connection as errored and log it
	if err != nil {
		logrus.WithError(err).WithField("http_request", r).Error("error while handling IVR")
		return conn, ivr.HandleAsFailure(ctx, rt.DB, svc, conn, w, err, clog)
	}

	return conn, nil
}

// handleStatus handles all incoming IVR events / status updates
func handleStatus(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, ch *models.Channel, svc ivr.Service, r *http.Request, w http.ResponseWriter, clog *models.ChannelLog) (*models.ChannelConnection, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second*55)
	defer cancel()

	// preprocess this status
	body, err := svc.PreprocessStatus(ctx, rt, r)
	if err != nil {
		return nil, svc.WriteErrorResponse(w, errors.Wrapf(err, "error while preprocessing status"))
	}
	if len(body) > 0 {
		contentType := httpx.DetectContentType(body)
		w.Header().Set("Content-Type", contentType)
		_, err := w.Write(body)
		return nil, err
	}

	// get our external id
	externalID, err := svc.CallIDForRequest(r)
	if err != nil {
		return nil, svc.WriteErrorResponse(w, errors.Wrapf(err, "unable to get call id for request"))
	}

	// load our connection
	conn, err := models.SelectChannelConnectionByExternalID(ctx, rt.DB, ch.ID(), models.ConnectionTypeIVR, externalID)
	if errors.Cause(err) == sql.ErrNoRows {
		return nil, svc.WriteEmptyResponse(w, "unknown connection, ignoring")
	}
	if err != nil {
		return nil, svc.WriteErrorResponse(w, errors.Wrapf(err, "unable to load channel connection with id: %s", externalID))
	}

	err = ivr.HandleIVRStatus(ctx, rt, oa, svc, conn, r, w, clog)

	// had an error? mark our connection as errored and log it
	if err != nil {
		logrus.WithError(err).WithField("http_request", r).Error("error while handling status")
		return conn, ivr.HandleAsFailure(ctx, rt.DB, svc, conn, w, err, clog)
	}

	return conn, nil
}
