package ivr

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"time"

	"github.com/nyaruka/gocommon/httpx"
	"github.com/nyaruka/gocommon/storage"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/excellent/types"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/resumes"
	"github.com/nyaruka/goflow/flows/triggers"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/config"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/runner"
	"github.com/nyaruka/mailroom/runtime"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type CallID string

const (
	NilCallID     = CallID("")
	NilAttachment = utils.Attachment("")

	// ErrorMessage that is spoken to an IVR user if an error occurs
	ErrorMessage = "An error has occurred, please try again later."
)

// CallEndedError is our constant error for when a call has ended
var CallEndedError = fmt.Errorf("call ended")

// our map of client constructors
var constructors = make(map[models.ChannelType]ClientConstructor)

// ClientConstructor defines our signature for creating a new IVR client from a channel
type ClientConstructor func(*http.Client, *models.Channel) (Client, error)

// RegisterClientType registers the passed in channel type with the passed in constructor
func RegisterClientType(channelType models.ChannelType, constructor ClientConstructor) {
	constructors[channelType] = constructor
}

// GetClient creates the right kind of IVRClient for the passed in channel
func GetClient(channel *models.Channel) (Client, error) {
	constructor := constructors[channel.Type()]
	if constructor == nil {
		return nil, errors.Errorf("no ivr client for channel type: %s", channel.Type())
	}

	return constructor(http.DefaultClient, channel)
}

// Client defines the interface IVR clients must satisfy
type Client interface {
	RequestCall(number urns.URN, handleURL string, statusURL string) (CallID, *httpx.Trace, error)

	HangupCall(externalID string) (*httpx.Trace, error)

	WriteSessionResponse(ctx context.Context, rp *redis.Pool, channel *models.Channel, conn *models.ChannelConnection, session *models.Session, number urns.URN, resumeURL string, req *http.Request, w http.ResponseWriter) error

	WriteErrorResponse(w http.ResponseWriter, err error) error

	WriteEmptyResponse(w http.ResponseWriter, msg string) error

	ResumeForRequest(r *http.Request) (Resume, error)

	StatusForRequest(r *http.Request) (models.ConnectionStatus, int)

	PreprocessResume(ctx context.Context, db *sqlx.DB, rp *redis.Pool, conn *models.ChannelConnection, r *http.Request) ([]byte, error)

	PreprocessStatus(ctx context.Context, db *sqlx.DB, rp *redis.Pool, r *http.Request) ([]byte, error)

	ValidateRequestSignature(r *http.Request) error

	DownloadMedia(url string) (*http.Response, error)

	URNForRequest(r *http.Request) (urns.URN, error)

	CallIDForRequest(r *http.Request) (string, error)
}

// HangupCall hangs up the passed in call also taking care of updating the status of our call in the process
func HangupCall(ctx context.Context, config *config.Config, db *sqlx.DB, conn *models.ChannelConnection) error {
	// no matter what mark our call as failed
	defer conn.MarkFailed(ctx, db, time.Now())

	// load our org assets
	oa, err := models.GetOrgAssets(ctx, db, conn.OrgID())
	if err != nil {
		return errors.Wrapf(err, "unable to load org")
	}

	// and our channel
	channel := oa.ChannelByID(conn.ChannelID())
	if channel == nil {
		return errors.Wrapf(err, "unable to load channel")
	}

	// create the right client
	c, err := GetClient(channel)
	if err != nil {
		return errors.Wrapf(err, "unable to create ivr client")
	}

	// try to request our call hangup
	trace, err := c.HangupCall(conn.ExternalID())

	// insert an channel log if we have an HTTP trace
	if trace != nil {
		desc := "Hangup Requested"
		isError := false
		if trace.Response == nil || trace.Response.StatusCode/100 != 2 {
			desc = "Error Hanging up Call"
			isError = true
		}
		log := models.NewChannelLog(trace, isError, desc, channel, conn)
		err := models.InsertChannelLogs(ctx, db, []*models.ChannelLog{log})

		// log any error inserting our channel log, but try to continue
		if err != nil {
			logrus.WithError(err).Error("error inserting channel log")
		}
	}

	if err != nil {
		return errors.Wrapf(err, "error hanging call up")
	}

	return nil
}

// RequestCallStart creates a new ChannelSession for the passed in flow start and contact, returning the created session
func RequestCallStart(ctx context.Context, config *config.Config, db *sqlx.DB, oa *models.OrgAssets, start *models.FlowStartBatch, contact *models.Contact) (*models.ChannelConnection, error) {
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

	// create our channel connection
	conn, err := models.InsertIVRConnection(
		ctx, db, oa.OrgID(), channel.ID(), start.StartID(), contact.ID(), models.URNID(urnID),
		models.ConnectionDirectionOut, models.ConnectionStatusPending, "",
	)
	if err != nil {
		return nil, errors.Wrapf(err, "error creating ivr session")
	}

	return conn, RequestCallStartForConnection(ctx, config, db, channel, telURN, conn)
}

func RequestCallStartForConnection(ctx context.Context, config *config.Config, db *sqlx.DB, channel *models.Channel, telURN urns.URN, conn *models.ChannelConnection) error {
	// the domain that will be used for callbacks, can be specific for channels due to white labeling
	domain := channel.ConfigValue(models.ChannelConfigCallbackDomain, config.Domain)

	// get max concurrent events if any
	maxCalls := channel.ConfigValue(models.ChannelConfigMaxConcurrentEvents, "")
	if maxCalls != "" {
		maxCalls, _ := strconv.Atoi(maxCalls)

		// max calls is set, lets see how many are currently active on this channel
		if maxCalls > 0 {
			count, err := models.ActiveChannelConnectionCount(ctx, db, channel.ID())
			if err != nil {
				return errors.Wrapf(err, "error finding number of active channel connections")
			}

			// we are at max calls, do not move on
			if count >= maxCalls {
				logrus.WithField("channel_id", channel.ID()).Info("call being queued, max concurrent reached")
				err := conn.MarkThrottled(ctx, db, time.Now())
				if err != nil {
					return errors.Wrapf(err, "error marking connection as throttled")
				}
				return nil
			}
		}
	}

	// create our callback
	form := url.Values{
		"connection": []string{fmt.Sprintf("%d", conn.ID())},
		"start":      []string{fmt.Sprintf("%d", conn.StartID())},
		"action":     []string{"start"},
		"urn":        []string{telURN.String()},
	}

	resumeURL := fmt.Sprintf("https://%s/mr/ivr/c/%s/handle?%s", domain, channel.UUID(), form.Encode())
	statusURL := fmt.Sprintf("https://%s/mr/ivr/c/%s/status", domain, channel.UUID())

	// create the right client
	c, err := GetClient(channel)
	if err != nil {
		return errors.Wrapf(err, "unable to create ivr client")
	}

	// try to request our call start
	callID, trace, err := c.RequestCall(telURN, resumeURL, statusURL)

	/// insert an channel log if we have an HTTP trace
	if trace != nil {
		desc := "Call Requested"
		isError := false
		if trace.Response == nil || trace.Response.StatusCode/100 != 2 {
			desc = "Error Requesting Call"
			isError = true
		}
		log := models.NewChannelLog(trace, isError, desc, channel, conn)
		err := models.InsertChannelLogs(ctx, db, []*models.ChannelLog{log})

		// log any error inserting our channel log, but try to continue
		if err != nil {
			logrus.WithError(err).Error("error inserting channel log")
		}
	}

	if err != nil {
		// set our status as errored
		err := conn.UpdateStatus(ctx, db, models.ConnectionStatusFailed, 0, time.Now())
		if err != nil {
			return errors.Wrapf(err, "error setting errored status on session")
		}
		return nil
	}

	// update our channel session
	err = conn.UpdateExternalID(ctx, db, string(callID))
	if err != nil {
		return errors.Wrapf(err, "error updating session external id")
	}

	return nil
}

// WriteErrorResponse marks the passed in connection as errored and writes the appropriate error response to our writer
func WriteErrorResponse(ctx context.Context, db *sqlx.DB, client Client, conn *models.ChannelConnection, w http.ResponseWriter, rootErr error) error {
	err := conn.MarkFailed(ctx, db, time.Now())
	if err != nil {
		logrus.WithError(err).Error("error when trying to mark connection as errored")
	}
	return client.WriteErrorResponse(w, rootErr)
}

// StartIVRFlow takes care of starting the flow in the passed in start for the passed in contact and URN
func StartIVRFlow(
	ctx context.Context, rt *runtime.Runtime, client Client, resumeURL string, oa *models.OrgAssets,
	channel *models.Channel, conn *models.ChannelConnection, c *models.Contact, urn urns.URN, startID models.StartID,
	r *http.Request, w http.ResponseWriter) error {

	// connection isn't in a wired status, that's an error
	if conn.Status() != models.ConnectionStatusWired && conn.Status() != models.ConnectionStatusInProgress {
		return WriteErrorResponse(ctx, rt.DB, client, conn, w, errors.Errorf("connection in invalid state: %s", conn.Status()))
	}

	// check that the call that has been created has a valid state
	status, _ := client.StatusForRequest(r)
	if status == models.ConnectionStatusErrored {
		return errors.Errorf("new call not in valid state: %s", status)
	}

	// get the flow for our start
	start, err := models.GetFlowStartAttributes(ctx, rt.DB, startID)
	if err != nil {
		return errors.Wrapf(err, "unable to load start: %d", startID)
	}

	flow, err := oa.FlowByID(start.FlowID())
	if err != nil {
		return errors.Wrapf(err, "unable to load flow: %d", startID)
	}

	// our flow contact
	contact, err := c.FlowContact(oa)
	if err != nil {
		return errors.Wrapf(err, "error loading flow contact")
	}

	var params *types.XObject
	if len(start.Extra()) > 0 {
		params, err = types.ReadXObject(start.Extra())
		if err != nil {
			return errors.Wrap(err, "unable to read JSON from flow start extra")
		}
	}

	var history *flows.SessionHistory
	if len(start.SessionHistory()) > 0 {
		history, err = models.ReadSessionHistory(start.SessionHistory())
		if err != nil {
			return errors.Wrap(err, "unable to read JSON from flow start history")
		}
	}

	// our builder for the triggers that will be created for contacts
	flowRef := assets.NewFlowReference(flow.UUID(), flow.Name())

	var trigger flows.Trigger
	if len(start.ParentSummary()) > 0 {
		trigger = triggers.NewBuilder(oa.Env(), flowRef, contact).
			FlowAction(history, start.ParentSummary()).
			WithConnection(channel.ChannelReference(), urn).
			Build()
	} else {
		trigger = triggers.NewBuilder(oa.Env(), flowRef, contact).
			Manual().
			WithConnection(channel.ChannelReference(), urn).
			WithParams(params).
			Build()
	}

	// mark our connection as started
	err = conn.MarkStarted(ctx, rt.DB, time.Now())
	if err != nil {
		return errors.Wrapf(err, "error updating call status")
	}

	// we set the connection on the session before our event hooks fire so that IVR messages can be created with the right connection reference
	hook := func(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, oa *models.OrgAssets, sessions []*models.Session) error {
		for _, session := range sessions {
			session.SetChannelConnection(conn)
		}
		return nil
	}

	// start our flow
	sessions, err := runner.StartFlowForContacts(ctx, rt, oa, flow, []flows.Trigger{trigger}, hook, true)
	if err != nil {
		return errors.Wrapf(err, "error starting flow")
	}

	if len(sessions) == 0 {
		return errors.Errorf("no ivr session created")
	}

	// have our client output our session status
	err = client.WriteSessionResponse(ctx, rt.RP, channel, conn, sessions[0], urn, resumeURL, r, w)
	if err != nil {
		return errors.Wrapf(err, "error writing ivr response for start")
	}

	return nil
}

// ResumeIVRFlow takes care of resuming the flow in the passed in start for the passed in contact and URN
func ResumeIVRFlow(
	ctx context.Context, rt *runtime.Runtime,
	resumeURL string, client Client,
	oa *models.OrgAssets, channel *models.Channel, conn *models.ChannelConnection, c *models.Contact, urn urns.URN,
	r *http.Request, w http.ResponseWriter) error {

	contact, err := c.FlowContact(oa)
	if err != nil {
		return errors.Wrapf(err, "error creating flow contact")
	}

	session, err := models.ActiveSessionForContact(ctx, rt.DB, rt.SessionStorage, oa, models.FlowTypeVoice, contact)
	if err != nil {
		return errors.Wrapf(err, "error loading session for contact")
	}

	if session == nil {
		return WriteErrorResponse(ctx, rt.DB, client, conn, w, errors.Errorf("no active IVR session for contact"))
	}

	if session.ConnectionID() == nil {
		return WriteErrorResponse(ctx, rt.DB, client, conn, w, errors.Errorf("active session: %d has no connection", session.ID()))
	}

	if *session.ConnectionID() != conn.ID() {
		return WriteErrorResponse(ctx, rt.DB, client, conn, w, errors.Errorf("active session: %d does not match connection: %d", session.ID(), *session.ConnectionID()))
	}

	// preprocess this request
	body, err := client.PreprocessResume(ctx, rt.DB, rt.RP, conn, r)
	if err != nil {
		return errors.Wrapf(err, "error preprocessing resume")
	}

	if body != nil {
		// guess our content type and set it
		contentType := http.DetectContentType(body)
		w.Header().Set("Content-Type", contentType)
		_, err := w.Write(body)
		return err
	}

	// hook to set our connection on our session before our event hooks run
	hook := func(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, oa *models.OrgAssets, sessions []*models.Session) error {
		for _, session := range sessions {
			session.SetChannelConnection(conn)
		}
		return nil
	}

	// make sure our call is still happening
	status, _ := client.StatusForRequest(r)
	if status != models.ConnectionStatusInProgress {
		err := conn.UpdateStatus(ctx, rt.DB, status, 0, time.Now())
		if err != nil {
			return errors.Wrapf(err, "error updating status")
		}
	}

	// get the input of our request
	ivrResume, err := client.ResumeForRequest(r)
	if err != nil {
		// call has ended, so will our session
		if err == CallEndedError {
			WriteErrorResponse(ctx, rt.DB, client, conn, w, errors.Wrapf(err, "call already ended"))
		}

		return WriteErrorResponse(ctx, rt.DB, client, conn, w, errors.Wrapf(err, "error finding input for request"))
	}

	var resume flows.Resume
	var clientErr error
	switch res := ivrResume.(type) {
	case InputResume:
		resume, clientErr, err = buildMsgResume(ctx, rt.Config, rt.DB, rt.RP, rt.MediaStorage, client, channel, contact, urn, conn, oa, r, res)

	case DialResume:
		resume, clientErr, err = buildDialResume(oa, contact, res)

	default:
		return fmt.Errorf("unknown resume type: %vvv", ivrResume)
	}

	if err != nil {
		return errors.Wrapf(err, "error building resume for request")
	}
	if clientErr != nil {
		return client.WriteErrorResponse(w, clientErr)
	}
	if resume == nil {
		return client.WriteErrorResponse(w, fmt.Errorf("no resume found, ending call"))
	}

	session, err = runner.ResumeFlow(ctx, rt, oa, session, resume, hook)
	if err != nil {
		return errors.Wrapf(err, "error resuming ivr flow")
	}

	// if still active, write out our response
	if status == models.ConnectionStatusInProgress {
		err = client.WriteSessionResponse(ctx, rt.RP, channel, conn, session, urn, resumeURL, r, w)
		if err != nil {
			return errors.Wrapf(err, "error writing ivr response for resume")
		}
	} else {
		err = models.ExitSessions(ctx, rt.DB, []models.SessionID{session.ID()}, models.ExitCompleted, time.Now())
		if err != nil {
			logrus.WithError(err).Error("error closing session")
		}

		return client.WriteErrorResponse(w, fmt.Errorf("call completed"))
	}

	return nil
}

func buildDialResume(oa *models.OrgAssets, contact *flows.Contact, resume DialResume) (flows.Resume, error, error) {
	return resumes.NewDial(oa.Env(), contact, flows.NewDial(resume.Status, resume.Duration)), nil, nil
}

func buildMsgResume(
	ctx context.Context, config *config.Config, db *sqlx.DB, rp *redis.Pool, store storage.Storage,
	client Client, channel *models.Channel, contact *flows.Contact, urn urns.URN,
	conn *models.ChannelConnection, oa *models.OrgAssets, r *http.Request, resume InputResume) (flows.Resume, error, error) {
	// our msg UUID
	msgUUID := flows.MsgUUID(uuids.New())

	// we have an attachment, download it locally
	if resume.Attachment != NilAttachment {
		var err error
		var resp *http.Response
		for retry := 0; retry < 45; retry++ {
			resp, err = client.DownloadMedia(resume.Attachment.URL())
			if err == nil && resp.StatusCode == 200 {
				break
			}
			time.Sleep(time.Second)

			if resp != nil {
				logrus.WithField("retry", retry).WithField("status", resp.StatusCode).WithField("url", resume.Attachment.URL()).Info("retrying download of attachment")
			} else {
				logrus.WithError(err).WithField("retry", retry).WithField("url", resume.Attachment.URL()).Info("retrying download of attachment")
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

		resume.Attachment, err = oa.Org().StoreAttachment(ctx, store, filename, resume.Attachment.ContentType(), resp.Body)
		if err != nil {
			return nil, errors.Wrapf(err, "unable to download and store attachment, ending call"), nil
		}
	}

	attachments := []utils.Attachment{}
	if resume.Attachment != NilAttachment {
		attachments = []utils.Attachment{resume.Attachment}
	}

	msgIn := flows.NewMsgIn(msgUUID, urn, channel.ChannelReference(), resume.Input, attachments)

	// create an incoming message
	msg := models.NewIncomingIVR(oa.OrgID(), conn, msgIn, time.Now())

	// allocate a topup for this message if org uses topups)
	topupID, err := models.AllocateTopups(ctx, db, rp, oa.Org(), 1)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "error allocating topup for incoming IVR message")
	}

	msg.SetTopup(topupID)

	// commit it
	err = models.InsertMessages(ctx, db, []*models.Msg{msg})
	if err != nil {
		return nil, nil, errors.Wrapf(err, "error committing new message")
	}

	// create our msg resume event
	return resumes.NewMsg(oa.Env(), contact, msgIn), nil, nil
}

// HandleIVRStatus is called on status callbacks for an IVR call. We let the client decide whether the call has
// ended for some reason and update the state of the call and session if so
func HandleIVRStatus(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, client Client, conn *models.ChannelConnection, r *http.Request, w http.ResponseWriter) error {
	// read our status and duration from our client
	status, duration := client.StatusForRequest(r)

	// if we errored schedule a retry if appropriate
	if status == models.ConnectionStatusErrored {
		// no associated start? this is a permanent failure
		if conn.StartID() == models.NilStartID {
			conn.MarkFailed(ctx, rt.DB, time.Now())
			return client.WriteEmptyResponse(w, "status updated: F")
		}

		// on errors we need to look up the flow to know how long to wait before retrying
		start, err := models.GetFlowStartAttributes(ctx, rt.DB, conn.StartID())
		if err != nil {
			return errors.Wrapf(err, "unable to load start: %d", conn.StartID())
		}

		flow, err := oa.FlowByID(start.FlowID())
		if err != nil {
			return errors.Wrapf(err, "unable to load flow: %d", start.FlowID())
		}

		conn.MarkErrored(ctx, rt.DB, time.Now(), flow.IVRRetryWait())

		if conn.Status() == models.ConnectionStatusErrored {
			return client.WriteEmptyResponse(w, fmt.Sprintf("status updated: %s next_attempt: %s", conn.Status(), conn.NextAttempt()))
		}
	} else if status == models.ConnectionStatusFailed {
		conn.MarkFailed(ctx, rt.DB, time.Now())
	} else {
		if status != conn.Status() || duration > 0 {
			err := conn.UpdateStatus(ctx, rt.DB, status, duration, time.Now())
			if err != nil {
				return errors.Wrapf(err, "error updating call status")
			}
		}
	}

	return client.WriteEmptyResponse(w, fmt.Sprintf("status updated: %s", status))
}
