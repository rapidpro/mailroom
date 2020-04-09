package nexmo

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rsa"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/goflow/flows/routers/waits"
	"github.com/nyaruka/goflow/flows/routers/waits/hints"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/goflow/utils/uuids"
	"github.com/nyaruka/mailroom/ivr"
	"github.com/nyaruka/mailroom/models"

	"github.com/buger/jsonparser"
	jwt "github.com/dgrijalva/jwt-go"
	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// BaseURL for Nexmo calls, public so our main IVR test can change it
var BaseURL = `https://api.nexmo.com/v1/calls`

// IgnoreSignatures sets whether we ignore signatures (for unit tests)
var IgnoreSignatures = false

const (
	nexmoChannelType = models.ChannelType("NX")

	gatherTimeout = 30
	recordTimeout = 600

	appIDConfig      = "nexmo_app_id"
	privateKeyConfig = "nexmo_app_private_key"

	errorBody = `<?xml version="1.0" encoding="UTF-8"?>
	<Response>
		<Say>An error was encountered. Goodbye.</Say>
		<Hangup></Hangup>
	</Response>
	`

	statusFailed = "failed"
)

var indentMarshal = true

type client struct {
	channel    *models.Channel
	baseURL    string
	appID      string
	privateKey *rsa.PrivateKey
}

func init() {
	ivr.RegisterClientType(nexmoChannelType, NewClientFromChannel)
}

// NewClientFromChannel creates a new Twilio IVR client for the passed in account and and auth token
func NewClientFromChannel(channel *models.Channel) (ivr.Client, error) {
	appID := channel.ConfigValue(appIDConfig, "")
	key := channel.ConfigValue(privateKeyConfig, "")
	if appID == "" || key == "" {
		return nil, errors.Errorf("missing %s or %s on channel config", appIDConfig, privateKeyConfig)
	}

	privateKey, err := jwt.ParseRSAPrivateKeyFromPEM([]byte(key))
	if err != nil {
		return nil, errors.Wrapf(err, "error parsing private key")
	}

	return &client{
		channel:    channel,
		baseURL:    BaseURL,
		appID:      appID,
		privateKey: privateKey,
	}, nil
}

func readBody(r *http.Request) ([]byte, error) {
	if r.Body == http.NoBody {
		return nil, nil
	}
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, nil
	}
	r.Body = ioutil.NopCloser(bytes.NewBuffer(body))
	return body, nil
}

func (c *client) CallIDForRequest(r *http.Request) (string, error) {
	// get our recording url out
	body, err := readBody(r)
	if err != nil {
		return "", errors.Wrapf(err, "error reading body from request")
	}
	callID, err := jsonparser.GetString(body, "uuid")
	if err != nil {
		return "", errors.Errorf("invalid json body")
	}

	if callID == "" {
		return "", errors.Errorf("no uuid set on call")
	}
	return callID, nil
}

func (c *client) URNForRequest(r *http.Request) (urns.URN, error) {
	// get our recording url out
	body, err := readBody(r)
	if err != nil {
		return "", errors.Wrapf(err, "error reading body from request")
	}
	direction, _ := jsonparser.GetString(body, "direction")
	if direction == "" {
		direction = "inbound"
	}

	urnKey := ""
	switch direction {
	case "inbound":
		urnKey = "from"
	case "outbound":
		urnKey = "to"
	}

	urn, err := jsonparser.GetString(body, urnKey)
	if err != nil {
		return "", errors.Errorf("invalid json body")
	}

	if urn == "" {
		return "", errors.Errorf("no urn found in body")
	}
	return urns.NewTelURNForCountry("+"+urn, "")
}

func (c *client) DownloadMedia(url string) (*http.Response, error) {
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	token, err := c.generateToken()
	if err != nil {
		return nil, errors.Wrapf(err, "error generating jwt token")
	}

	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	return http.DefaultClient.Do(req)
}

func (c *client) PreprocessResume(ctx context.Context, db *sqlx.DB, rp *redis.Pool, conn *models.ChannelConnection, r *http.Request) ([]byte, error) {
	// if this is a recording_url resume, grab that
	waitType := r.URL.Query().Get("wait_type")

	switch waitType {
	case "record":
		recordingUUID := r.URL.Query().Get("recording_uuid")
		if recordingUUID == "" {
			return nil, errors.Errorf("record resume without recording_uuid")
		}

		rc := rp.Get()
		defer rc.Close()

		redisKey := fmt.Sprintf("recording_%s", recordingUUID)
		recordingURL, err := redis.String(rc.Do("get", redisKey))
		if err != nil && err != redis.ErrNil {
			return nil, errors.Wrapf(err, "error getting recording url from redis")
		}

		// found a URL, stuff it in our request and move on
		if recordingURL != "" {
			r.URL.RawQuery = "&recording_url=" + url.QueryEscape(recordingURL)
			logrus.WithField("recording_url", recordingURL).Info("found recording URL")
			rc.Do("del", redisKey)
			return nil, nil
		}

		// no recording yet, send back another 1 second input / wait
		path := r.URL.RequestURI()
		proxyPath := r.Header.Get("X-Forwarded-Path")
		if proxyPath != "" {
			path = proxyPath
		}
		url := fmt.Sprintf("https://%s%s", r.Host, path)

		input := &Input{
			Action:       "input",
			Timeout:      1,
			SubmitOnHash: true,
			EventURL:     []string{url},
			EventMethod:  http.MethodPost,
		}
		return json.MarshalIndent([]interface{}{input}, "", "  ")

	case "recording_url":
		// this is our async callback for our recording URL, we stuff it in redis and return an empty response
		recordingUUID := r.URL.Query().Get("recording_uuid")
		if recordingUUID == "" {
			return nil, errors.Errorf("recording_url resume without recording_uuid")
		}

		// get our recording url out
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			return nil, errors.Wrapf(err, "error reading body from request")
		}
		recordingURL, err := jsonparser.GetString(body, "recording_url")
		if err != nil {
			return nil, errors.Errorf("invalid json body")
		}
		if recordingURL == "" {
			return nil, errors.Errorf("no recording_url found in request")
		}

		// write it to redis
		rc := rp.Get()
		defer rc.Close()

		redisKey := fmt.Sprintf("recording_%s", recordingUUID)
		_, err = rc.Do("setex", redisKey, 300, recordingURL)
		if err != nil {
			return nil, errors.Wrapf(err, "error inserting recording URL into redis")
		}

		msgBody := map[string]string{
			"_message": fmt.Sprintf("inserted recording url: %s for uuid: %s", recordingURL, recordingUUID),
		}
		return json.MarshalIndent(msgBody, "", "  ")

	default:
		return nil, nil
	}
}

type Phone struct {
	Type   string `json:"type"`
	Number int    `json:"number"`
}

type CallRequest struct {
	To           []Phone  `json:"to"`
	From         Phone    `json:"from"`
	AnswerURL    []string `json:"answer_url"`
	AnswerMethod string   `json:"answer_method"`
	EventURL     []string `json:"event_url"`
	EventMethod  string   `json:"event_method"`
}

// CallResponse is our struct for a Nexmo call response
// {
//  "uuid": "63f61863-4a51-4f6b-86e1-46edebcf9356",
//  "status": "started",
//  "direction": "outbound",
//  "conversation_uuid": "CON-f972836a-550f-45fa-956c-12a2ab5b7d22"
// }
type CallResponse struct {
	UUID             string `json:"uuid"`
	Status           string `json:"status"`
	Direction        string `json:"direction"`
	ConversationUUID string `json:"conversation_uuid"`
}

// RequestCall causes this client to request a new outgoing call for this provider
func (c *client) RequestCall(client *http.Client, number urns.URN, resumeURL string, statusURL string) (ivr.CallID, error) {
	callR := &CallRequest{
		AnswerURL:    []string{resumeURL + "&sig=" + url.QueryEscape(c.calculateSignature(resumeURL))},
		AnswerMethod: http.MethodPost,

		EventURL:    []string{statusURL + "?sig=" + url.QueryEscape(c.calculateSignature(statusURL))},
		EventMethod: http.MethodPost,
	}
	rawTo, err := strconv.Atoi(number.Path())
	if err != nil {
		return ivr.NilCallID, errors.Wrapf(err, "unable to turn urn path into number: %s", number.Path())
	}
	callR.To = append(callR.To, Phone{Type: "phone", Number: rawTo})

	rawFrom, err := strconv.Atoi(c.channel.Address())
	if err != nil {
		return ivr.NilCallID, errors.Wrapf(err, "unable to turn urn path into number: %s", number.Path())
	}
	callR.From = Phone{Type: "phone", Number: rawFrom}

	resp, err := c.makeRequest(client, http.MethodPost, BaseURL, callR)
	if err != nil {
		return ivr.NilCallID, errors.Wrapf(err, "error trying to start call")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		io.Copy(ioutil.Discard, resp.Body)
		return ivr.NilCallID, errors.Errorf("received non 200 status for call start: %d", resp.StatusCode)
	}

	// read our body
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return ivr.NilCallID, errors.Wrapf(err, "error reading response body")
	}

	// parse out our call sid
	call := &CallResponse{}
	err = json.Unmarshal(body, call)
	if err != nil || call.UUID == "" {
		return ivr.NilCallID, errors.Errorf("unable to read call uuid")
	}

	if call.Status == statusFailed {
		return ivr.NilCallID, errors.Errorf("call status returned as failed")
	}

	logrus.WithField("body", string(body)).WithField("status", resp.StatusCode).Debug("requested call")

	return ivr.CallID(call.UUID), nil
}

// HangupCall asks Nexmo to hang up the call that is passed in
func (c *client) HangupCall(client *http.Client, callID string) error {
	hangupBody := map[string]string{"action": "hangup"}
	url := BaseURL + "/" + callID
	resp, err := c.makeRequest(client, http.MethodPut, url, hangupBody)
	if err != nil {
		return errors.Wrapf(err, "error trying to hangup call")
	}
	defer resp.Body.Close()
	io.Copy(ioutil.Discard, resp.Body)

	if resp.StatusCode != 204 {
		return errors.Errorf("received non 204 status for call hangup: %d", resp.StatusCode)
	}
	return nil
}

type NCCOInput struct {
	DTMF             string `json:"dtmf"`
	TimedOut         bool   `json:"timed_out"`
	UUID             string `json:"uuid"`
	ConversationUUID string `json:"conversation_uuid"`
	Timestamp        string `json:"timestamp"`
}

// InputForRequest returns the input for the passed in request, if any
func (c *client) InputForRequest(r *http.Request) (string, utils.Attachment, error) {
	// parse our input
	input := &NCCOInput{}
	bb, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return "", ivr.NilAttachment, errors.Wrapf(err, "error reading request body")
	}

	err = json.Unmarshal(bb, input)
	if err != nil {
		return "", ivr.NilAttachment, errors.Wrapf(err, "unable to parse ncco request")
	}

	// this could be empty, in which case we return nothing at all
	empty := r.Form.Get("empty")
	if empty == "true" {
		return "", ivr.NilAttachment, nil
	}

	// otherwise grab the right field based on our wait type
	waitType := r.Form.Get("wait_type")
	switch waitType {
	case "gather":
		// this could be a timeout, in which case we return nothing at all
		if input.TimedOut {
			return "", ivr.NilAttachment, nil
		}

		return input.DTMF, ivr.NilAttachment, nil
	case "record":
		recordingURL := r.URL.Query().Get("recording_url")
		if recordingURL == "" {
			return "", ivr.NilAttachment, nil
		}
		logrus.WithField("recording_url", recordingURL).Info("input found recording")
		return "", utils.Attachment("audio:" + recordingURL), nil
	default:
		return "", ivr.NilAttachment, errors.Errorf("unknown wait_type: %s", waitType)
	}
}

type StatusRequest struct {
	UUID     string `json:"uuid"`
	Status   string `json:"status"`
	Duration string `json:"duration"`
}

// StatusForRequest returns the current call status for the passed in status (and optional duration if known)
func (c *client) StatusForRequest(r *http.Request) (models.ConnectionStatus, int) {
	// this is a resume, call is in progress, no need to look at the body
	if r.Form.Get("action") == "resume" {
		return models.ConnectionStatusInProgress, 0
	}

	status := &StatusRequest{}
	bb, err := ioutil.ReadAll(r.Body)
	if err != nil {
		logrus.WithError(err).Error("error reading status request body")
		return models.ConnectionStatusErrored, 0
	}
	err = json.Unmarshal(bb, status)
	if err != nil {
		logrus.WithError(err).WithField("body", string(bb)).Error("error unmarshalling ncco status")
		return models.ConnectionStatusErrored, 0
	}

	switch status.Status {

	case "started", "ringing":
		return models.ConnectionStatusWired, 0

	case "answered":
		return models.ConnectionStatusInProgress, 0

	case "completed":
		duration, _ := strconv.Atoi(status.Duration)
		return models.ConnectionStatusCompleted, duration

	case "rejected", "busy", "unanswered", "timeout", "failed", "machine":
		return models.ConnectionStatusErrored, 0

	default:
		logrus.WithField("status", status.Status).Error("unknown call status in ncco callback")
		return models.ConnectionStatusFailed, 0
	}
}

// ValidateRequestSignature validates the signature on the passed in request, returning an error if it is invaled
func (c *client) ValidateRequestSignature(r *http.Request) error {
	if IgnoreSignatures {
		return nil
	}

	// only validate handling calls, we can't verify others
	if !strings.HasSuffix(r.URL.Path, "handle") {
		return nil
	}

	actual := r.URL.Query().Get("sig")
	if actual == "" {
		return errors.Errorf("missing request sig")
	}

	path := r.URL.RequestURI()
	proxyPath := r.Header.Get("X-Forwarded-Path")
	if proxyPath != "" {
		path = proxyPath
	}

	url := fmt.Sprintf("https://%s%s", r.Host, path)
	expected := c.calculateSignature(url)
	if expected != actual {
		return errors.Errorf("mismatch in signatures for url: %s, actual: %s, expected: %s", url, actual, expected)
	}
	return nil
}

// WriteSessionResponse writes a TWIML response for the events in the passed in session
func (c *client) WriteSessionResponse(session *models.Session, number urns.URN, resumeURL string, r *http.Request, w http.ResponseWriter) error {
	// for errored sessions we should just output our error body
	if session.Status() == models.SessionStatusFailed {
		return errors.Errorf("cannot write IVR response for failed session")
	}

	// otherwise look for any say events
	sprint := session.Sprint()
	if sprint == nil {
		return errors.Errorf("cannot write IVR response for session with no sprint")
	}

	// get our response
	response, err := c.responseForSprint(resumeURL, session.Wait(), sprint.Events())
	if err != nil {
		return errors.Wrap(err, "unable to build response for IVR call")
	}

	_, err = w.Write([]byte(response))
	if err != nil {
		return errors.Wrap(err, "error writing IVR response")
	}

	return nil
}

// WriteErrorResponse writes an error / unavailable response
func (c *client) WriteErrorResponse(w http.ResponseWriter, err error) error {
	actions := []interface{}{Talk{
		Action: "talk",
		Text:   ivr.ErrorMessage,
		Error:  err.Error(),
	}}
	body, err := json.Marshal(actions)
	if err != nil {
		return errors.Wrapf(err, "error marshalling ncco error")
	}

	_, err = w.Write(body)
	return err
}

// WriteEmptyResponse writes an empty (but valid) response
func (c *client) WriteEmptyResponse(w http.ResponseWriter, msg string) error {
	msgBody := map[string]string{
		"_message": msg,
	}
	body, err := json.Marshal(msgBody)
	if err != nil {
		return errors.Wrapf(err, "error marshalling ncco message")
	}

	_, err = w.Write(body)
	return err
}

func (c *client) makeRequest(client *http.Client, method string, sendURL string, body interface{}) (*http.Response, error) {
	bb, err := json.Marshal(body)
	if err != nil {
		return nil, errors.Wrapf(err, "error json encoding request")
	}

	req, _ := http.NewRequest(method, sendURL, bytes.NewReader(bb))
	token, err := c.generateToken()
	if err != nil {
		return nil, errors.Wrapf(err, "error generating jwt token")
	}

	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")

	return client.Do(req)
}

// calculateSignature calculates a signature for the passed in URL
func (c *client) calculateSignature(u string) string {
	url, _ := url.Parse(u)

	var buffer bytes.Buffer
	buffer.WriteString(url.Scheme)
	buffer.WriteString("://")
	buffer.WriteString(url.Host)
	buffer.WriteString(url.Path)

	form := url.Query()
	keys := make(sort.StringSlice, 0, len(form))
	for k := range form {
		keys = append(keys, k)
	}
	keys.Sort()

	for _, k := range keys {
		// ignore sig parameter
		if k == "sig" {
			continue
		}

		buffer.WriteString(k)
		for _, v := range form[k] {
			buffer.WriteString(v)
		}
	}

	// hash with SHA1
	mac := hmac.New(sha1.New, []byte(c.appID))
	mac.Write(buffer.Bytes())
	hash := mac.Sum(nil)

	// encode with Base64
	encoded := make([]byte, base64.StdEncoding.EncodedLen(len(hash)))
	base64.StdEncoding.Encode(encoded, hash)

	return string(encoded)
}

type jwtClaims struct {
	ApplicationID string `json:"application_id"`
	jwt.StandardClaims
}

func (c *client) generateToken() (string, error) {
	claims := jwtClaims{
		c.appID,
		jwt.StandardClaims{
			Id:       strconv.Itoa(rand.Int()),
			IssuedAt: time.Now().UTC().Unix(),
		},
	}
	token := jwt.NewWithClaims(jwt.GetSigningMethod("RS256"), claims)
	return token.SignedString(c.privateKey)
}

// NCCO building utilities

type Talk struct {
	Action  string `json:"action"`
	Text    string `json:"text"`
	BargeIn bool   `json:"bargeIn,omitempty"`
	Error   string `json:"_error,omitempty"`
	Message string `json:"_message,omitempty"`
}

type Stream struct {
	Action    string   `json:"action"`
	StreamURL []string `json:"streamUrl"`
}

type Hangup struct {
	XMLName string `xml:"Hangup"`
}

type Redirect struct {
	XMLName string `xml:"Redirect"`
	URL     string `xml:",chardata"`
}

type Input struct {
	Action       string   `json:"action"`
	MaxDigits    int      `json:"maxDigits,omitempty"`
	SubmitOnHash bool     `json:"submitOnHash"`
	Timeout      int      `json:"timeOut"`
	EventURL     []string `json:"eventUrl"`
	EventMethod  string   `json:"eventMethod"`
}

type Record struct {
	Action       string   `json:"action"`
	EndOnKey     string   `json:"endOnKey,omitempty"`
	Timeout      int      `json:"timeOut,omitempty"`
	EndOnSilence int      `json:"endOnSilence,omitempty"`
	EventURL     []string `json:"eventUrl"`
	EventMethod  string   `json:"eventMethod"`
}

func (c *client) responseForSprint(resumeURL string, w flows.ActivatedWait, es []flows.Event) (string, error) {
	actions := make([]interface{}, 0, 1)
	waitActions := make([]interface{}, 0, 1)

	if w != nil {
		msgWait, isMsgWait := w.(*waits.ActivatedMsgWait)
		if !isMsgWait {
			return "", errors.Errorf("unable to use wait of type: %s in IVR call", w.Type())
		}

		switch hint := msgWait.Hint().(type) {
		case *hints.DigitsHint:
			eventURL := resumeURL + "&wait_type=gather"
			eventURL = eventURL + "&sig=" + url.QueryEscape(c.calculateSignature(eventURL))
			input := &Input{
				Action:       "input",
				Timeout:      gatherTimeout,
				SubmitOnHash: true,
				EventURL:     []string{eventURL},
				EventMethod:  http.MethodPost,
			}
			// limit our digits if asked to
			if hint.Count != nil {
				input.MaxDigits = *hint.Count
			} else {
				input.MaxDigits = 20
			}
			waitActions = append(waitActions, input)

		case *hints.AudioHint:
			// Nexmo is goofy in that they do not synchronously send us recordings. Rather the move on in
			// the NCCO script immediately and then asynchronously call the event URL on the record URL
			// when the recording is ready.
			//
			// We deal with this by adding the record event with a status callback including a UUID
			// which we will store the recording url under when it is received. Meanwhile we put an input
			// with a 1 second timeout in the script that will get called / repeated until the UUID is
			// populated at which time we will actually continue.

			recordingUUID := string(uuids.New())
			eventURL := resumeURL + "&wait_type=recording_url&recording_uuid=" + recordingUUID
			eventURL = eventURL + "&sig=" + url.QueryEscape(c.calculateSignature(eventURL))
			record := &Record{
				Action:       "record",
				EventURL:     []string{eventURL},
				EventMethod:  http.MethodPost,
				EndOnKey:     "#",
				Timeout:      recordTimeout,
				EndOnSilence: 5,
			}
			waitActions = append(waitActions, record)

			// nexmo is goofy in that they do not call our event URL upon gathering the recording but
			// instead move on. So we need to put in an input here as well
			eventURL = resumeURL + "&wait_type=record&recording_uuid=" + recordingUUID
			eventURL = eventURL + "&sig=" + url.QueryEscape(c.calculateSignature(eventURL))
			input := &Input{
				Action:       "input",
				Timeout:      1,
				SubmitOnHash: true,
				EventURL:     []string{eventURL},
				EventMethod:  http.MethodPost,
			}
			waitActions = append(waitActions, input)

		default:
			return "", errors.Errorf("unable to use wait in IVR call, unknow type: %s", msgWait.Hint().Type())
		}
	}

	isWaitInput := false
	if len(waitActions) > 0 {
		_, isWaitInput = waitActions[0].(*Input)
	}

	for _, e := range es {
		switch event := e.(type) {
		case *events.IVRCreatedEvent:
			if len(event.Msg.Attachments()) == 0 {
				actions = append(actions, Talk{
					Action:  "talk",
					Text:    event.Msg.Text(),
					BargeIn: isWaitInput,
				})
			} else {
				for _, a := range event.Msg.Attachments() {
					actions = append(actions, Stream{
						Action:    "stream",
						StreamURL: []string{a.URL()},
					})
				}
			}
		}
	}

	for _, w := range waitActions {
		actions = append(actions, w)
	}

	var body []byte
	var err error
	if indentMarshal {
		body, err = json.MarshalIndent(actions, "", "  ")
	} else {
		body, err = json.Marshal(actions)
	}
	if err != nil {
		return "", errors.Wrap(err, "unable to marshal ncco body")
	}

	return string(body), nil
}
