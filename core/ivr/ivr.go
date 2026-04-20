package ivr

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/gocommon/httpx"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/excellent/types"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/resumes"
	"github.com/nyaruka/goflow/flows/triggers"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/runner"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/null/v3"
	"github.com/pkg/errors"
)

type CallID string

const (
	NilCallID     = CallID("")
	NilAttachment = utils.Attachment("")

	// ErrorMessage that is spoken to an IVR user if an error occurs
	ErrorMessage = "An error has occurred, please try again later."
)

// our map of service constructors
var constructors = make(map[models.ChannelType]ServiceConstructor)

// ServiceConstructor defines our signature for creating a new IVR service from a channel
type ServiceConstructor func(*http.Client, *models.Channel) (Service, error)

// RegisterServiceType registers the passed in channel type with the passed in constructor
func RegisterServiceType(channelType models.ChannelType, constructor ServiceConstructor) {
	constructors[channelType] = constructor
}

// GetService creates the right kind of IVR service for the passed in channel
func GetService(channel *models.Channel) (Service, error) {
	constructor := constructors[channel.Type()]
	if constructor == nil {
		return nil, errors.Errorf("no IVR service for channel type: %s", channel.Type())
	}

	return constructor(http.DefaultClient, channel)
}

// Service defines the interface IVR services must satisfy
type Service interface {
	RequestCall(number urns.URN, handleURL string, statusURL string, machineDetection bool) (CallID, *httpx.Trace, error)

	HangupCall(externalID string) (*httpx.Trace, error)

	WriteSessionResponse(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, channel *models.Channel, call *models.Call, session *models.Session, number urns.URN, resumeURL string, req *http.Request, w http.ResponseWriter) error
	WriteRejectResponse(w http.ResponseWriter) error
	WriteErrorResponse(w http.ResponseWriter, err error) error
	WriteEmptyResponse(w http.ResponseWriter, msg string) error

	ResumeForRequest(r *http.Request) (Resume, error)

	// StatusForRequest returns the call status for the passed in request, and if it's an error the reason,
	// and if available, the current call duration
	StatusForRequest(r *http.Request) (models.CallStatus, models.CallError, int)

	// CheckStartRequest checks the start request from the service is as we expect and if not returns an error reason
	CheckStartRequest(r *http.Request) models.CallError

	PreprocessResume(ctx context.Context, rt *runtime.Runtime, call *models.Call, r *http.Request) ([]byte, error)

	PreprocessStatus(ctx context.Context, rt *runtime.Runtime, r *http.Request) ([]byte, error)

	ValidateRequestSignature(r *http.Request) error

	DownloadMedia(url string) (*http.Response, error)

	URNForRequest(r *http.Request) (urns.URN, error)

	CallIDForRequest(r *http.Request) (string, error)

	RedactValues(*models.Channel) []string
}

// HangupCall hangs up the passed in call also taking care of updating the status of our call in the process
func HangupCall(ctx context.Context, rt *runtime.Runtime, call *models.Call) (*models.ChannelLog, error) {
	// no matter what mark our call as failed
	defer call.MarkFailed(ctx, rt.DB, time.Now())

	// load our org assets
	oa, err := models.GetOrgAssets(ctx, rt, call.OrgID())
	if err != nil {
		return nil, errors.Wrapf(err, "unable to load org")
	}

	// and our channel
	channel := oa.ChannelByID(call.ChannelID())
	if channel == nil {
		return nil, errors.Wrapf(err, "unable to load channel")
	}

	// create the right service
	svc, err := GetService(channel)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to create IVR service")
	}

	clog := models.NewChannelLog(models.ChannelLogTypeIVRHangup, channel, svc.RedactValues(channel))
	defer clog.End()

	// try to request our call hangup
	trace, err := svc.HangupCall(call.ExternalID())
	if trace != nil {
		clog.HTTP(trace)
	}
	if err != nil {
		clog.Error(err)
	}

	if err := call.AttachLog(ctx, rt.DB, clog); err != nil {
		slog.Error("error attaching ivr channel log", "error", err)
	}

	return clog, err
}

// RequestCall creates a new ChannelSession for the passed in flow start and contact, returning the created session
func RequestCall(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, start *models.FlowStartBatch, contact *models.Contact) (*models.Call, error) {
	// find a tel URL for the contact
	telURN := urns.NilURN
	for _, u := range contact.URNs() {
		if u.Scheme() == urns.TelScheme {
			telURN = u
		}
	}

	if telURN == urns.NilURN {
		return nil, errors.Errorf("no tel URN on contact, cannot start IVR flow")
	}

	// get the ID of our URN
	urnID := models.GetURNInt(telURN, "id")
	if urnID == 0 {
		return nil, errors.Errorf("no urn id for URN: %s, cannot start IVR flow", telURN)
	}

	// build our channel assets, we need these to calculate the preferred channel for a call
	channels, err := oa.Channels()
	if err != nil {
		return nil, errors.Wrapf(err, "unable to load channels for org")
	}
	ca := flows.NewChannelAssets(channels)

	urn, err := flows.ParseRawURN(ca, telURN, assets.IgnoreMissing)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to parse URN: %s", telURN)
	}

	// get the channel to use for outgoing calls
	callChannel := ca.GetForURN(urn, assets.ChannelRoleCall)
	if callChannel == nil {
		// can't start call, no channel that can call
		return nil, nil
	}

	hasCall := callChannel.HasRole(assets.ChannelRoleCall)
	if !hasCall {
		return nil, nil
	}

	// get the channel for this URN
	channel := callChannel.Asset().(*models.Channel)

	// create our call object
	conn, err := models.InsertCall(
		ctx, rt.DB, oa.OrgID(), channel.ID(), start.StartID, contact.ID(), models.URNID(urnID),
		models.CallDirectionOut, models.CallStatusPending, "",
	)
	if err != nil {
		return nil, errors.Wrapf(err, "error creating call")
	}

	clog, err := RequestStartForCall(ctx, rt, channel, telURN, conn)

	// log any error inserting our channel log, but continue
	if clog != nil {
		if err := models.InsertChannelLogs(ctx, rt, []*models.ChannelLog{clog}); err != nil {
			slog.Error("error inserting channel log", "error", err)
		}
	}

	return conn, err
}

func RequestStartForCall(ctx context.Context, rt *runtime.Runtime, channel *models.Channel, telURN urns.URN, call *models.Call) (*models.ChannelLog, error) {
	// the domain that will be used for callbacks, can be specific for channels due to white labeling
	domain := channel.ConfigValue(models.ChannelConfigCallbackDomain, rt.Config.Domain)

	// get max concurrent events if any
	maxCalls := channel.ConfigValue(models.ChannelConfigMaxConcurrentEvents, "")
	if maxCalls != "" {
		maxCalls, _ := strconv.Atoi(maxCalls)

		// max calls is set, lets see how many are currently active on this channel
		if maxCalls > 0 {
			count, err := models.ActiveCallCount(ctx, rt.DB, channel.ID())
			if err != nil {
				return nil, errors.Wrapf(err, "error finding number of active calls")
			}

			// we are at max calls, do not move on
			if count >= maxCalls {
				slog.Info("call being queued, max concurrent reached", "channel_id", channel.ID())
				err := call.MarkThrottled(ctx, rt.DB, time.Now())
				if err != nil {
					return nil, errors.Wrapf(err, "error marking call as throttled")
				}
				return nil, nil
			}
		}
	}

	// create our callback
	form := url.Values{
		"connection": []string{fmt.Sprintf("%d", call.ID())},
		"start":      []string{fmt.Sprintf("%d", call.StartID())},
		"action":     []string{"start"},
		"urn":        []string{telURN.String()},
	}

	resumeURL := fmt.Sprintf("https://%s/mr/ivr/c/%s/handle?%s", domain, channel.UUID(), form.Encode())
	statusURL := fmt.Sprintf("https://%s/mr/ivr/c/%s/status", domain, channel.UUID())

	// create the right service
	svc, err := GetService(channel)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to create IVR service")
	}

	clog := models.NewChannelLog(models.ChannelLogTypeIVRStart, channel, svc.RedactValues(channel))
	defer clog.End()

	// try to request our call start
	callID, trace, err := svc.RequestCall(telURN, resumeURL, statusURL, channel.MachineDetection())
	if trace != nil {
		clog.HTTP(trace)
	}
	if err != nil {
		clog.Error(err)

		// set our status as errored
		err := call.UpdateStatus(ctx, rt.DB, models.CallStatusFailed, 0, time.Now())
		if err != nil {
			return clog, errors.Wrapf(err, "error setting errored status on session")
		}
		return clog, nil
	}

	// update our channel session
	if err := call.UpdateExternalID(ctx, rt.DB, string(callID)); err != nil {
		return clog, errors.Wrapf(err, "error updating session external id")
	}
	if err := call.AttachLog(ctx, rt.DB, clog); err != nil {
		slog.Error("error attaching ivr channel log", "error", err)
	}

	return clog, nil
}

// HandleAsFailure marks the passed in call as errored and writes the appropriate error response to our writer
func HandleAsFailure(ctx context.Context, db *sqlx.DB, svc Service, call *models.Call, w http.ResponseWriter, rootErr error) error {
	err := call.MarkFailed(ctx, db, time.Now())
	if err != nil {
		slog.Error("error marking call as failed", "error", err)
	}
	return svc.WriteErrorResponse(w, rootErr)
}

// StartIVRFlow takes care of starting the flow in the passed in start for the passed in contact and URN
func StartIVRFlow(
	ctx context.Context, rt *runtime.Runtime, svc Service, resumeURL string, oa *models.OrgAssets,
	channel *models.Channel, call *models.Call, c *models.Contact, urn urns.URN, startID models.StartID,
	r *http.Request, w http.ResponseWriter) error {

	// call isn't in a wired or in-progress status then we shouldn't be here
	if call.Status() != models.CallStatusWired && call.Status() != models.CallStatusInProgress {
		return HandleAsFailure(ctx, rt.DB, svc, call, w, errors.Errorf("call in invalid state: %s", call.Status()))
	}

	// get the flow for our start
	start, err := models.GetFlowStartAttributes(ctx, rt.DB, startID)
	if err != nil {
		return errors.Wrapf(err, "unable to load start: %d", startID)
	}
	flow, err := oa.FlowByID(start.FlowID)
	if err != nil {
		return errors.Wrapf(err, "unable to load flow: %d", startID)
	}

	// check that call on service side is in the state we need to continue
	if errorReason := svc.CheckStartRequest(r); errorReason != "" {
		err := call.MarkErrored(ctx, rt.DB, dates.Now(), flow.IVRRetryWait(), errorReason)
		if err != nil {
			return errors.Wrap(err, "unable to mark call as errored")
		}

		errMsg := fmt.Sprintf("status updated: %s", call.Status())
		if call.Status() == models.CallStatusErrored {
			errMsg = fmt.Sprintf("%s, next_attempt: %s", errMsg, call.NextAttempt())
		}

		return svc.WriteErrorResponse(w, errors.New(errMsg))
	}

	// our flow contact
	contact, err := c.FlowContact(oa)
	if err != nil {
		return errors.Wrapf(err, "error loading flow contact")
	}

	var params *types.XObject
	if !start.Params.IsNull() {
		params, err = types.ReadXObject(start.Params)
		if err != nil {
			return errors.Wrap(err, "unable to read JSON from flow start params")
		}
	}

	var history *flows.SessionHistory
	if !start.SessionHistory.IsNull() {
		history, err = models.ReadSessionHistory(start.SessionHistory)
		if err != nil {
			return errors.Wrap(err, "unable to read JSON from flow start history")
		}
	}

	// our builder for the triggers that will be created for contacts
	flowRef := assets.NewFlowReference(flow.UUID(), flow.Name())

	var trigger flows.Trigger
	if !start.ParentSummary.IsNull() {
		trigger = triggers.NewBuilder(oa.Env(), flowRef, contact).
			FlowAction(history, json.RawMessage(start.ParentSummary)).
			WithCall(channel.Reference(), urn).
			Build()
	} else {
		trigger = triggers.NewBuilder(oa.Env(), flowRef, contact).
			Manual().
			WithCall(channel.Reference(), urn).
			WithParams(params).
			Build()
	}

	// mark our call as started
	err = call.MarkStarted(ctx, rt.DB, time.Now())
	if err != nil {
		return errors.Wrapf(err, "error updating call status")
	}

	// we set the call on the session before our event hooks fire so that IVR messages can be created with the right call reference
	hook := func(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, oa *models.OrgAssets, sessions []*models.Session) error {
		for _, session := range sessions {
			session.SetCall(call)
		}
		return nil
	}

	// start our flow
	sessions, err := runner.StartFlowForContacts(ctx, rt, oa, flow, []*models.Contact{c}, []flows.Trigger{trigger}, hook, true)
	if err != nil {
		return errors.Wrapf(err, "error starting flow")
	}

	if len(sessions) == 0 {
		return errors.Errorf("no ivr session created")
	}

	// have our service output our session status
	err = svc.WriteSessionResponse(ctx, rt, oa, channel, call, sessions[0], urn, resumeURL, r, w)
	if err != nil {
		return errors.Wrapf(err, "error writing ivr response for start")
	}

	return nil
}

// ResumeIVRFlow takes care of resuming the flow in the passed in start for the passed in contact and URN
func ResumeIVRFlow(
	ctx context.Context, rt *runtime.Runtime,
	resumeURL string, svc Service,
	oa *models.OrgAssets, channel *models.Channel, call *models.Call, c *models.Contact, urn urns.URN,
	r *http.Request, w http.ResponseWriter) error {

	contact, err := c.FlowContact(oa)
	if err != nil {
		return errors.Wrapf(err, "error creating flow contact")
	}

	session, err := models.FindWaitingSessionForContact(ctx, rt.DB, rt.SessionStorage, oa, models.FlowTypeVoice, contact)
	if err != nil {
		return errors.Wrapf(err, "error loading session for contact")
	}

	if session == nil {
		return HandleAsFailure(ctx, rt.DB, svc, call, w, errors.Errorf("no active IVR session for contact"))
	}

	if session.CallID() == nil {
		return HandleAsFailure(ctx, rt.DB, svc, call, w, errors.Errorf("active session: %d has no call", session.ID()))
	}
	if *session.CallID() != call.ID() {
		return HandleAsFailure(ctx, rt.DB, svc, call, w, errors.Errorf("active session: %d does not match call: %d", session.ID(), *session.CallID()))
	}

	// check if call has been marked as errored - it maybe have been updated by status callback
	if call.Status() == models.CallStatusErrored || call.Status() == models.CallStatusFailed {
		err = models.ExitSessions(ctx, rt.DB, []models.SessionID{session.ID()}, models.SessionStatusInterrupted)
		if err != nil {
			slog.Error("error interrupting session", "error", err)
		}

		return svc.WriteErrorResponse(w, fmt.Errorf("ending call due to previous status callback"))
	}

	// preprocess this request
	body, err := svc.PreprocessResume(ctx, rt, call, r)
	if err != nil {
		return errors.Wrapf(err, "error preprocessing resume")
	}

	if body != nil {
		// guess our content type and set it
		contentType, _ := httpx.DetectContentType(body)
		w.Header().Set("Content-Type", contentType)
		_, err := w.Write(body)
		return err
	}

	// hook to set our call on our session before our event hooks run
	hook := func(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, oa *models.OrgAssets, sessions []*models.Session) error {
		for _, session := range sessions {
			session.SetCall(call)
		}
		return nil
	}

	// make sure our call is still happening
	status, _, _ := svc.StatusForRequest(r)
	if status != models.CallStatusInProgress {
		err := call.UpdateStatus(ctx, rt.DB, status, 0, time.Now())
		if err != nil {
			return errors.Wrapf(err, "error updating status")
		}
	}

	// get the input of our request
	ivrResume, err := svc.ResumeForRequest(r)
	if err != nil {
		return HandleAsFailure(ctx, rt.DB, svc, call, w, errors.Wrapf(err, "error finding input for request"))
	}

	var resume flows.Resume
	var svcErr error
	switch res := ivrResume.(type) {
	case InputResume:
		resume, svcErr, err = buildMsgResume(ctx, rt, svc, channel, contact, urn, call, oa, r, res)
		if resume != nil {
			session.SetIncomingMsg(models.MsgID(resume.(*resumes.MsgResume).Msg().ID()), null.NullString)
		}

	case DialResume:
		resume, svcErr, err = buildDialResume(oa, contact, res)

	default:
		return fmt.Errorf("unknown resume type: %vvv", ivrResume)
	}

	if err != nil {
		return errors.Wrapf(err, "error building resume for request")
	}
	if svcErr != nil {
		return svc.WriteErrorResponse(w, svcErr)
	}
	if resume == nil {
		return svc.WriteErrorResponse(w, fmt.Errorf("no resume found, ending call"))
	}

	session, err = runner.ResumeFlow(ctx, rt, oa, session, c, resume, hook)
	if err != nil {
		return errors.Wrapf(err, "error resuming ivr flow")
	}

	// if still active, write out our response
	if status == models.CallStatusInProgress {
		err = svc.WriteSessionResponse(ctx, rt, oa, channel, call, session, urn, resumeURL, r, w)
		if err != nil {
			return errors.Wrapf(err, "error writing ivr response for resume")
		}
	} else {
		err = models.ExitSessions(ctx, rt.DB, []models.SessionID{session.ID()}, models.SessionStatusCompleted)
		if err != nil {
			slog.Error("error closing session", "error", err)
		}

		return svc.WriteErrorResponse(w, fmt.Errorf("call completed"))
	}

	return nil
}

func buildDialResume(oa *models.OrgAssets, contact *flows.Contact, resume DialResume) (flows.Resume, error, error) {
	return resumes.NewDial(oa.Env(), contact, flows.NewDial(resume.Status, resume.Duration)), nil, nil
}

func buildMsgResume(
	ctx context.Context, rt *runtime.Runtime,
	svc Service, channel *models.Channel, contact *flows.Contact, urn urns.URN,
	call *models.Call, oa *models.OrgAssets, r *http.Request, resume InputResume) (flows.Resume, error, error) {
	// our msg UUID
	msgUUID := flows.MsgUUID(uuids.New())

	// we have an attachment, download it locally
	if resume.Attachment != NilAttachment {
		var err error
		var resp *http.Response
		for retry := 0; retry < 45; retry++ {
			resp, err = svc.DownloadMedia(resume.Attachment.URL())
			if err == nil && resp.StatusCode == 200 {
				break
			}
			time.Sleep(time.Second)

			if resp != nil {
				slog.Info("retrying download of attachment", "retry", retry, "status", resp.StatusCode, "url", resume.Attachment.URL())
			} else {
				slog.Info("retrying download of attachment", "error", err, "retry", retry, "url", resume.Attachment.URL())
			}
		}

		if err != nil {
			return nil, errors.Wrapf(err, "error downloading attachment, ending call"), nil
		}

		if resp == nil {
			return nil, errors.Errorf("unable to download attachment, ending call"), nil
		}

		// filename is based on our org id and msg UUID
		filename := string(msgUUID) + path.Ext(resume.Attachment.URL())

		resume.Attachment, err = oa.Org().StoreAttachment(ctx, rt, filename, resume.Attachment.ContentType(), resp.Body)
		if err != nil {
			return nil, errors.Wrapf(err, "unable to download and store attachment, ending call"), nil
		}
	}

	attachments := []utils.Attachment{}
	if resume.Attachment != NilAttachment {
		attachments = []utils.Attachment{resume.Attachment}
	}

	msgIn := flows.NewMsgIn(msgUUID, urn, channel.Reference(), resume.Input, attachments)

	// create an incoming message
	msg := models.NewIncomingIVR(rt.Config, oa.OrgID(), call, msgIn, time.Now())

	// commit it
	if err := models.InsertMessages(ctx, rt.DB, []*models.Msg{msg}); err != nil {
		return nil, nil, errors.Wrapf(err, "error committing new message")
	}

	// create our msg resume event
	return resumes.NewMsg(oa.Env(), contact, msgIn), nil, nil
}

// HandleIVRStatus is called on status callbacks for an IVR call. We let the service decide whether the call has
// ended for some reason and update the state of the call and session if so
func HandleIVRStatus(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, svc Service, call *models.Call, r *http.Request, w http.ResponseWriter) error {
	// read our status and duration from our service
	status, errorReason, duration := svc.StatusForRequest(r)

	if call.Status() == models.CallStatusErrored || call.Status() == models.CallStatusFailed {
		return svc.WriteEmptyResponse(w, fmt.Sprintf("status %s ignored, already errored", status))
	}

	// if we errored schedule a retry if appropriate
	if status == models.CallStatusErrored {

		// if this is an incoming call it won't have an associated start and we don't retry it so just fail permanently
		if call.StartID() == models.NilStartID {
			call.MarkFailed(ctx, rt.DB, time.Now())
			return svc.WriteEmptyResponse(w, "no flow start found, status updated: F")
		}

		// on errors we need to look up the flow to know how long to wait before retrying
		start, err := models.GetFlowStartAttributes(ctx, rt.DB, call.StartID())
		if err != nil {
			return errors.Wrapf(err, "unable to load start: %d", call.StartID())
		}

		flow, err := oa.FlowByID(start.FlowID)
		if err != nil {
			return errors.Wrapf(err, "unable to load flow: %d", start.FlowID)
		}

		call.MarkErrored(ctx, rt.DB, dates.Now(), flow.IVRRetryWait(), errorReason)

		if call.Status() == models.CallStatusErrored {
			return svc.WriteEmptyResponse(w, fmt.Sprintf("status updated: %s, next_attempt: %s", call.Status(), call.NextAttempt()))
		}

	} else if status == models.CallStatusFailed {
		call.MarkFailed(ctx, rt.DB, time.Now())
	} else {
		if status != call.Status() || duration > 0 {
			err := call.UpdateStatus(ctx, rt.DB, status, duration, time.Now())
			if err != nil {
				return errors.Wrapf(err, "error updating call status")
			}
		}
	}

	return svc.WriteEmptyResponse(w, fmt.Sprintf("status updated: %s", status))
}
