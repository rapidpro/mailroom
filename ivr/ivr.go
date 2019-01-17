package ivr

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/resumes"
	"github.com/nyaruka/goflow/flows/triggers"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/config"
	"github.com/nyaruka/mailroom/httputils"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/runner"
	"github.com/nyaruka/mailroom/s3utils"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type CallID string

const (
	NilCallID     = CallID("")
	NilAttachment = flows.Attachment("")
)

const userAgent = "Mailroom/"

// Message that is spoken to an IVR user if an error occurs
const ErrorMessage = "An error has occurred, please try again later."

// CallEndedError is our constant error for when a call has ended
var CallEndedError = fmt.Errorf("call ended")

// our map of client constructors
var constructors = make(map[models.ChannelType]ClientConstructor)

// ClientConstructor defines our signature for creating a new IVR client from a channel
type ClientConstructor func(c *models.Channel) (Client, error)

// RegisterClientType registers the passed in channel type with the passed in constructor
func RegisterClientType(channelType models.ChannelType, constructor ClientConstructor) {
	constructors[channelType] = constructor
}

// GetClient creates the right kind of IVRClient for the passed in channel
func GetClient(channel *models.Channel) (Client, error) {
	constructor := constructors[channel.Type()]
	if constructor == nil {
		return nil, errors.Errorf("no ivr client for chanel type: %s", channel.Type())
	}

	return constructor(channel)
}

// Client defines the interface IVR clients must satisfy
type Client interface {
	RequestCall(client *http.Client, number urns.URN, callbackURL string, statusURL string) (CallID, error)

	WriteSessionResponse(session *models.Session, resumeURL string, req *http.Request, w http.ResponseWriter) error

	WriteErrorResponse(w http.ResponseWriter, err error) error

	WriteEmptyResponse(w http.ResponseWriter, msg string) error

	InputForRequest(r *http.Request) (string, flows.Attachment, error)

	StatusForRequest(r *http.Request) (models.ConnectionStatus, int)

	PreprocessResume(ctx context.Context, db *sqlx.DB, rp *redis.Pool, conn *models.ChannelConnection, r *http.Request) ([]byte, error)

	ValidateRequestSignature(r *http.Request) error

	DownloadMedia(url string) (*http.Response, error)
}

// RequestCallStart creates a new ChannelSession for the passed in flow start and contact, returning the created session
func RequestCallStart(ctx context.Context, config *config.Config, db *sqlx.DB, org *models.OrgAssets, start *models.FlowStartBatch, contact *models.Contact) (*models.ChannelConnection, error) {
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
	channels, err := org.Channels()
	if err != nil {
		return nil, errors.Wrapf(err, "unable to load channels for org")
	}
	ca := flows.NewChannelAssets(channels)

	urn, err := flows.ParseRawURN(ca, telURN)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to parse URN: %s", telURN)
	}

	// get the channel to use for outgoing calls
	callChannel := ca.GetForURN(urn, assets.ChannelRoleCall)
	if callChannel == nil {
		// can't start call, no channel that can call
		return nil, nil
	}

	// get the channel for this URN
	channel := callChannel.Asset().(*models.Channel)

	// create our session
	conn, err := models.InsertIVRConnection(
		ctx, db, org.OrgID(), channel.ID(), start.StartID(), contact.ID(), models.URNID(urnID),
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

	// create our callback
	form := url.Values{
		"connection": []string{fmt.Sprintf("%d", conn.ID())},
		"start":      []string{fmt.Sprintf("%d", conn.StartID().Int64)},
		"action":     []string{"start"},
		"urn":        []string{telURN.String()},
	}

	startURL := fmt.Sprintf("https://%s/mr/ivr/handle?%s", domain, form.Encode())

	form["action"] = []string{"status"}
	statusURL := fmt.Sprintf("https://%s/mr/ivr/handle?%s", domain, form.Encode())

	// create the right client
	c, err := GetClient(channel)
	if err != nil {
		return errors.Wrapf(err, "unable to create ivr client")
	}

	// we create our own HTTP client with our own transport so we can log the request and set our user agent
	logger := httputils.NewLoggingTransport(http.DefaultTransport)
	client := &http.Client{Transport: httputils.NewUserAgentTransport(logger, userAgent+config.Version)}

	// try to request our call start
	callID, err := c.RequestCall(client, telURN, startURL, statusURL)

	// insert any logged requests
	for _, rt := range logger.RoundTrips {
		desc := "Call Requested"
		isError := false
		if rt.Status/100 != 2 {
			desc = "Error Requesting Call"
			isError = true
		}
		_, err := models.InsertChannelLog(
			ctx, db, desc, isError,
			rt.Method, rt.URL, rt.RequestBody, rt.Status, rt.ResponseBody,
			rt.StartedOn, rt.Elapsed,
			conn,
		)

		// log any error inserting our channel log, but try to continue
		if err != nil {
			logrus.WithError(err).Error("error inserting channel log")
		}
	}

	if err != nil {
		logrus.WithError(err).Error("error placing outbound call")

		// set our status as errored
		err := conn.UpdateStatus(ctx, db, models.ConnectionStatusFailed, 0, time.Now())
		if err != nil {
			return errors.Wrapf(err, "error setting errored status on session")
		}
		return nil
	}

	// update our channel session and return it
	err = conn.UpdateExternalID(ctx, db, string(callID))
	if err != nil {
		return errors.Wrapf(err, "error updating session external id")
	}

	return nil
}

// WriteErrorResponse marks the passed in connection as errored and writes the appropriate error response to our writer
func WriteErrorResponse(ctx context.Context, db *sqlx.DB, client Client, conn *models.ChannelConnection, w http.ResponseWriter, rootErr error) error {
	// TODO: mark our session as complete as well
	err := conn.MarkFailed(ctx, db, time.Now())
	if err != nil {
		logrus.WithError(err).Error("error when trying to mark connection as errored")
	}
	return client.WriteErrorResponse(w, rootErr)
}

// StartIVRFlow takes care of starting the flow in the passed in start for the passed in contact and URN
func StartIVRFlow(
	ctx context.Context, db *sqlx.DB, rp *redis.Pool, client Client, resumeURL string, org *models.OrgAssets,
	channel *models.Channel, conn *models.ChannelConnection, c *models.Contact, urn urns.URN, startID models.StartID,
	r *http.Request, w http.ResponseWriter) error {

	// connection isn't in a wired status, that's an error
	if conn.Status() != models.ConnectionStatusWired {
		return WriteErrorResponse(ctx, db, client, conn, w, errors.Errorf("connection in invalid state: %s", conn.Status()))
	}

	// get the flow for our start
	flowID, err := models.FlowIDForStart(ctx, db, org.OrgID(), startID)
	if err != nil {
		return errors.Wrapf(err, "unable to load start: %d", startID.Int64)
	}

	flow, err := org.FlowByID(flowID)
	if err != nil {
		return errors.Wrapf(err, "unable to load flow: %d", startID.Int64)
	}

	// build our session assets
	sa, err := models.GetSessionAssets(org)
	if err != nil {
		return errors.Wrapf(err, "error starting flow, unable to load assets")
	}

	// our flow contact
	contact, err := c.FlowContact(org, sa)
	if err != nil {
		return errors.Wrapf(err, "error loading flow contact")
	}

	// our builder for the triggers that will be created for contacts
	flowRef := assets.NewFlowReference(flow.UUID(), flow.Name())
	connRef := flows.NewConnection(channel.ChannelReference(), urn)
	trigger := triggers.NewManualVoiceTrigger(org.Env(), flowRef, contact, connRef, nil)

	// we set the connection on the session before our event hooks fire so that IVR messages can be created with the right connection reference
	hook := func(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, sessions []*models.Session) error {
		for _, session := range sessions {
			session.SetChannelConnection(conn)
		}
		return nil
	}

	// start our flow
	sessions, err := runner.StartFlowForContacts(ctx, db, rp, org, sa, []flows.Trigger{trigger}, hook, true)
	if err != nil {
		return errors.Wrapf(err, "error starting flow")
	}

	if len(sessions) == 0 {
		return errors.Errorf("no ivr session created")
	}

	// have our client output our session status
	err = client.WriteSessionResponse(sessions[0], resumeURL, r, w)
	if err != nil {
		return errors.Wrapf(err, "error writing ivr response for start")
	}

	// mark our connection as started
	err = conn.MarkStarted(ctx, db, time.Now())
	if err != nil {
		return errors.Wrapf(err, "error updating call status")
	}

	return nil
}

// ResumeIVRFlow takes care of resuming the flow in the passed in start for the passed in contact and URN
func ResumeIVRFlow(
	ctx context.Context, config *config.Config, db *sqlx.DB, rp *redis.Pool, s3Client s3iface.S3API,
	resumeURL string, client Client,
	org *models.OrgAssets, channel *models.Channel, conn *models.ChannelConnection, c *models.Contact, urn urns.URN,
	r *http.Request, w http.ResponseWriter) error {

	// connection isn't in progress, that's an error
	if conn.Status() != models.ConnectionStatusInProgress {
		return WriteErrorResponse(ctx, db, client, conn, w, errors.Errorf("connection in invalid status: %s", conn.Status()))
	}

	// build our session assets
	sa, err := models.GetSessionAssets(org)
	if err != nil {
		return errors.Wrapf(err, "unable to load assets")
	}

	contact, err := c.FlowContact(org, sa)
	if err != nil {
		return errors.Wrapf(err, "error creating flow contact")
	}

	session, err := models.ActiveSessionForContact(ctx, db, org, contact)
	if err != nil {
		return errors.Wrapf(err, "error loading session for contact")
	}

	if session.ConnectionID == nil {
		return WriteErrorResponse(ctx, db, client, conn, w, errors.Errorf("active session: %d has no connection", session.ID))
	}

	if *session.ConnectionID != conn.ID() {
		return WriteErrorResponse(ctx, db, client, conn, w, errors.Errorf("active session: %d does not match connetion: %d", session.ID, *session.ConnectionID))
	}

	// preprocess this request
	body, err := client.PreprocessResume(ctx, db, rp, conn, r)
	if err != nil {
		return errors.Wrapf(err, "error preprocessing resume")
	}

	if body != nil {
		_, err := w.Write(body)
		return err
	}

	// get the input of our request
	input, attachment, err := client.InputForRequest(r)
	if err != nil {
		// call has ended, so will our session
		if err == CallEndedError {
			err = models.ExitSessions(ctx, db, []models.SessionID{session.ID}, models.ExitCompleted, time.Now())
			if err != nil {
				return errors.Wrapf(err, "error marking sessions complete")
			}

			return client.WriteEmptyResponse(w, "session exited, ignoring resume")
		}

		return WriteErrorResponse(ctx, db, client, conn, w, errors.Wrapf(err, "unable finding input for request"))
	}

	// our msg UUID
	msgUUID := flows.MsgUUID(utils.NewUUID())

	// we have an attachment, download it locally
	if attachment != NilAttachment {
		var err error
		var resp *http.Response
		for retry := 0; retry < 45; retry++ {
			resp, err = client.DownloadMedia(attachment.URL())
			if err == nil && resp.StatusCode == 200 {
				break
			}
			time.Sleep(time.Second)
			logrus.WithError(err).WithField("retry", retry).WithField("status", resp.StatusCode).WithField("url", attachment.URL()).Info("retrying download of attachment")
		}

		if err != nil {
			return WriteErrorResponse(ctx, db, client, conn, w, errors.Wrapf(err, "error downloading attachment, ending call"))
		}

		if resp == nil {
			return WriteErrorResponse(ctx, db, client, conn, w, errors.Errorf("unable to download attachment, ending call"))
		}

		// download our body
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return WriteErrorResponse(ctx, db, client, conn, w, errors.Wrapf(err, "unable to download attachment body, ending call"))
		}
		resp.Body.Close()

		// check our content type
		contentType := http.DetectContentType(body)

		// filename is based on our org id and msg UUID
		filename := string(msgUUID) + path.Ext(attachment.URL())
		path := filepath.Join(config.S3MediaPrefix, fmt.Sprintf("%d", org.OrgID()), filename[:4], filename[4:8], filename)
		if !strings.HasPrefix(path, "/") {
			path = fmt.Sprintf("/%s", path)
		}

		// write to S3
		logrus.WithField("path", path).Info("** uploading s3 file")
		url, err := s3utils.PutS3File(s3Client, config.S3MediaBucket, path, contentType, body)
		if err != nil {
			return errors.Wrapf(err, "unable to write attachment to s3")
		}
		attachment = flows.Attachment(contentType + ":" + url)
	}

	attachments := []flows.Attachment{}
	if attachment != NilAttachment {
		attachments = []flows.Attachment{attachment}
	}

	msgIn := flows.NewMsgIn(msgUUID, urn, channel.ChannelReference(), input, attachments)

	// create an incoming message
	msg := models.NewIncomingIVR(org.OrgID(), conn, msgIn, time.Now())

	// find a topup
	rc := rp.Get()
	topupID, err := models.DecrementOrgCredits(ctx, db, rc, org.OrgID(), 1)
	rc.Close()

	// error or no topup, that's an end of call
	if err != nil {
		return errors.Wrapf(err, "unable to look up topup")
	}
	if topupID == models.NilTopupID {
		return client.WriteEmptyResponse(w, "no topups for org, exiting call")
	}
	msg.SetTopup(topupID)

	// commit it
	err = models.InsertMessages(ctx, db, []*models.Msg{msg})
	if err != nil {
		return errors.Wrapf(err, "error committing new message")
	}

	// create our msg resume event
	resume := resumes.NewMsgResume(org.Env(), contact, msgIn)

	// hook to set our connection on our session before our event hooks run
	hook := func(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, sessions []*models.Session) error {
		for _, session := range sessions {
			session.SetChannelConnection(conn)
		}
		return nil
	}

	session, err = runner.ResumeFlow(ctx, db, rp, org, sa, session, resume, hook)
	if err != nil {
		return errors.Wrapf(err, "error resuming ivr flow")
	}

	// have our client output our session status
	err = client.WriteSessionResponse(session, resumeURL, r, w)
	if err != nil {
		return errors.Wrapf(err, "error writing ivr response for resume")
	}

	return nil
}

// HandleIVRStatus is called on status callbacks for an IVR call. We let the client decide whether the call has
// ended for some reason and update the state of the call and session if so
func HandleIVRStatus(ctx context.Context, db *sqlx.DB, rp *redis.Pool, client Client, conn *models.ChannelConnection, r *http.Request, w http.ResponseWriter) error {
	// read our status and duration from our client
	status, duration := client.StatusForRequest(r)

	// if we errored, mark ourselves appropriately
	if status == models.ConnectionStatusErrored {
		conn.MarkErrored(ctx, db, time.Now())
	} else if status == models.ConnectionStatusFailed {
		conn.MarkFailed(ctx, db, time.Now())
	} else {
		if status != conn.Status() || duration > 0 {
			err := conn.UpdateStatus(ctx, db, status, duration, time.Now())
			if err != nil {
				return errors.Wrapf(err, "error updating call status")
			}
		}
	}

	return client.WriteEmptyResponse(w, fmt.Sprintf("status updated: %s", status))
}
