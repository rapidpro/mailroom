package ivr

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"time"

	"github.com/go-chi/chi"
	"github.com/nyaruka/gocommon/httpx"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/mailroom/core/ivr"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/tasks/handler"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/web"
	"github.com/pkg/errors"
)

func init() {
	web.RegisterRoute(http.MethodPost, "/mr/ivr/c/{uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}}/handle", newIVRHandler(handleCallback, models.ChannelLogTypeIVRCallback))
	web.RegisterRoute(http.MethodPost, "/mr/ivr/c/{uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}}/status", newIVRHandler(handleStatus, models.ChannelLogTypeIVRStatus))
	web.RegisterRoute(http.MethodPost, "/mr/ivr/c/{uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}}/incoming", newIVRHandler(handleIncoming, models.ChannelLogTypeIVRIncoming))
}

type ivrHandlerFn func(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, ch *models.Channel, svc ivr.Service, r *http.Request, w http.ResponseWriter) (*models.Call, error)

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

		recorder, err := httpx.NewRecorder(r, w, true)
		if err != nil {
			return svc.WriteErrorResponse(w, errors.Wrapf(err, "error reading request body"))
		}

		// validate this request's signature
		err = svc.ValidateRequestSignature(r)
		if err != nil {
			return svc.WriteErrorResponse(w, errors.Wrapf(err, "request failed signature validation"))
		}

		clog := models.NewChannelLogForIncoming(logType, ch, recorder, svc.RedactValues(ch))

		call, rerr := handler(ctx, rt, oa, ch, svc, r, recorder.ResponseWriter)
		if call != nil {
			if err := call.AttachLog(ctx, rt.DB, clog); err != nil {
				slog.Error("error attaching ivr channel log", "error", err, "http_request", r)
			}
		}

		if err := recorder.End(); err != nil {
			slog.Error("error recording IVR request", "error", err, "http_request", r)
		}

		clog.End()

		if err := models.InsertChannelLogs(ctx, rt, []*models.ChannelLog{clog}); err != nil {
			slog.Error("error writing ivr channel log", "error", err, "http_request", r)
		}

		return rerr
	}
}

func handleIncoming(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, ch *models.Channel, svc ivr.Service, r *http.Request, w http.ResponseWriter) (*models.Call, error) {
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
	event := models.NewChannelEvent(models.EventTypeIncomingCall, oa.OrgID(), ch.ID(), contact.ID(), urnID, models.NilOptInID, nil, false)

	externalID, err := svc.CallIDForRequest(r)
	if err != nil {
		return nil, svc.WriteErrorResponse(w, errors.Wrapf(err, "unable to get external id from request"))
	}

	// create our call
	call, err := models.InsertCall(ctx, rt.DB, oa.OrgID(), ch.ID(), models.NilStartID, contact.ID(), urnID, models.CallDirectionIn, models.CallStatusInProgress, externalID)
	if err != nil {
		return nil, svc.WriteErrorResponse(w, errors.Wrapf(err, "error creating call"))
	}

	// try to handle this event
	session, err := handler.HandleChannelEvent(ctx, rt, models.EventTypeIncomingCall, event, call)
	if err != nil {
		slog.Error("error handling incoming call", "error", err, "http_request", r)
		return call, svc.WriteErrorResponse(w, errors.Wrapf(err, "error handling incoming call"))
	}

	// if we matched with an incoming-call trigger, we'll have a session
	if session != nil {
		// that might have started a non-voice flow, in which case we need to reject this call
		if session.SessionType() != models.FlowTypeVoice {
			return call, svc.WriteRejectResponse(w)
		}

		// build our resume URL
		resumeURL := buildResumeURL(rt.Config, ch, call, urn)

		// have our client output our session status
		err = svc.WriteSessionResponse(ctx, rt, oa, ch, call, session, urn, resumeURL, r, w)
		if err != nil {
			return call, errors.Wrapf(err, "error writing ivr response for start")
		}

		return call, nil
	}

	// write our empty response
	return call, svc.WriteEmptyResponse(w, "missed call handled")
}

const (
	actionStart  = "start"
	actionResume = "resume"
	actionStatus = "status"
)

// IVRRequest is our form for what fields we expect in IVR callbacks
type IVRRequest struct {
	ConnectionID models.CallID `form:"connection" validate:"required"`
	Action       string        `form:"action"     validate:"required"`
}

// writeGenericErrorResponse is just a small utility method to write out a simple JSON error when we don't have a client yet
func writeGenericErrorResponse(w http.ResponseWriter, err error) error {
	return web.WriteMarshalled(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
}

func buildResumeURL(cfg *runtime.Config, channel *models.Channel, call *models.Call, urn urns.URN) string {
	domain := channel.ConfigValue(models.ChannelConfigCallbackDomain, cfg.Domain)
	form := url.Values{
		"action":     []string{actionResume},
		"connection": []string{fmt.Sprintf("%d", call.ID())},
		"urn":        []string{urn.String()},
	}

	return fmt.Sprintf("https://%s/mr/ivr/c/%s/handle?%s", domain, channel.UUID(), form.Encode())
}

// handles all incoming IVR requests related to a flow (status is handled elsewhere)
func handleCallback(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, ch *models.Channel, svc ivr.Service, r *http.Request, w http.ResponseWriter) (*models.Call, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second*55)
	defer cancel()

	request := &IVRRequest{}
	if err := web.DecodeAndValidateForm(request, r); err != nil {
		return nil, errors.Wrapf(err, "request failed validation")
	}

	// load our call
	conn, err := models.GetCallByID(ctx, rt.DB, oa.OrgID(), request.ConnectionID)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to load call with id: %d", request.ConnectionID)
	}

	// load our contact
	contact, err := models.LoadContact(ctx, rt.ReadonlyDB, oa, conn.ContactID())
	if err != nil {
		return conn, svc.WriteErrorResponse(w, errors.Wrapf(err, "no such contact"))
	}
	if contact.Status() != models.ContactStatusActive {
		return conn, svc.WriteErrorResponse(w, errors.Errorf("no contact with id: %d", conn.ContactID()))
	}

	// load the URN for this call
	urn, err := models.URNForID(ctx, rt.DB, oa, conn.ContactURNID())
	if err != nil {
		return conn, svc.WriteErrorResponse(w, errors.Errorf("unable to find call urn: %d", conn.ContactURNID()))
	}

	// make sure our URN is indeed present on our contact, no funny business
	found := false
	for _, u := range contact.URNs() {
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
		err = ivr.StartIVRFlow(ctx, rt, svc, resumeURL, oa, ch, conn, contact, urn, conn.StartID(), r, w)
	case actionResume:
		err = ivr.ResumeIVRFlow(ctx, rt, resumeURL, svc, oa, ch, conn, contact, urn, r, w)
	case actionStatus:
		err = ivr.HandleIVRStatus(ctx, rt, oa, svc, conn, r, w)

	default:
		err = svc.WriteErrorResponse(w, errors.Errorf("unknown action: %s", request.Action))
	}

	// had an error? mark our call as errored and log it
	if err != nil {
		slog.Error("error while handling IVR", "error", err, "http_request", r)
		return conn, ivr.HandleAsFailure(ctx, rt.DB, svc, conn, w, err)
	}

	return conn, nil
}

// handleStatus handles all incoming IVR events / status updates
func handleStatus(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, ch *models.Channel, svc ivr.Service, r *http.Request, w http.ResponseWriter) (*models.Call, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second*55)
	defer cancel()

	// preprocess this status
	body, err := svc.PreprocessStatus(ctx, rt, r)
	if err != nil {
		return nil, svc.WriteErrorResponse(w, errors.Wrapf(err, "error while preprocessing status"))
	}
	if len(body) > 0 {
		contentType, _ := httpx.DetectContentType(body)
		w.Header().Set("Content-Type", contentType)
		_, err := w.Write(body)
		return nil, err
	}

	// get our external id
	externalID, err := svc.CallIDForRequest(r)
	if err != nil {
		return nil, svc.WriteErrorResponse(w, errors.Wrapf(err, "unable to get call id for request"))
	}

	// load our call
	conn, err := models.GetCallByExternalID(ctx, rt.DB, ch.ID(), externalID)
	if errors.Cause(err) == sql.ErrNoRows {
		return nil, svc.WriteEmptyResponse(w, "unknown call, ignoring")
	}
	if err != nil {
		return nil, svc.WriteErrorResponse(w, errors.Wrapf(err, "unable to load call with id: %s", externalID))
	}

	err = ivr.HandleIVRStatus(ctx, rt, oa, svc, conn, r, w)

	// had an error? mark our call as errored and log it
	if err != nil {
		slog.Error("error while handling status", "error", err, "http_request", r)
		return conn, ivr.HandleAsFailure(ctx, rt.DB, svc, conn, w, err)
	}

	return conn, nil
}
