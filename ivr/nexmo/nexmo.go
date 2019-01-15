package nexmo

import (
	"bytes"
	"crypto/hmac"
	"crypto/rsa"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"time"

	jwt "github.com/dgrijalva/jwt-go"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/goflow/flows/waits"
	"github.com/nyaruka/goflow/flows/waits/hints"
	"github.com/nyaruka/mailroom/ivr"
	"github.com/nyaruka/mailroom/models"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	nexmoChannelType = models.ChannelType("NX")

	baseURL = `https://api.nexmo.com/v1/calls`

	inputTimeout = 120

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
		baseURL:    baseURL,
		appID:      appID,
		privateKey: privateKey,
	}, nil
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
func (c *client) RequestCall(client *http.Client, number urns.URN, callbackURL string, statusURL string) (ivr.CallID, error) {
	callR := &CallRequest{
		AnswerURL:    []string{callbackURL + "&sig=" + url.QueryEscape(c.calculateSignature(callbackURL))},
		AnswerMethod: http.MethodPost,

		EventURL:    []string{statusURL + "&sig=" + url.QueryEscape(c.calculateSignature(statusURL))},
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

	resp, err := c.postRequest(client, baseURL, callR)
	if err != nil {
		return ivr.NilCallID, errors.Wrapf(err, "error trying to start call")
	}

	if resp.StatusCode != 201 {
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

	logrus.WithField("body", body).WithField("status", resp.StatusCode).Debug("requested call")

	return ivr.CallID(call.UUID), nil
}

type NCCOInput struct {
	DTMF             string `json:"dtmf"`
	TimedOut         bool   `json:"timed_out"`
	UUID             string `json:"uuid"`
	ConversationUUID string `json:"conversation_uuid"`
	Timestamp        string `json:"timestamp"`
}

// InputForRequest returns the input for the passed in request, if any
func (c *client) InputForRequest(r *http.Request) (string, flows.Attachment, error) {
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

	// this could be a timeout, in which case we return nothing at all
	if input.TimedOut {
		return "", ivr.NilAttachment, nil
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
		return input.DTMF, ivr.NilAttachment, nil
	case "record":
		return "", flows.Attachment("audio:" + r.Form.Get("RecordingUrl")), nil
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
func (c *client) StatusForRequest(r *http.Request) (models.ChannelSessionStatus, int) {
	status := &StatusRequest{}
	bb, err := ioutil.ReadAll(r.Body)
	if err != nil {
		logrus.WithError(err).Error("error reading status request body")
		return models.ChannelSessionStatusErrored, 0
	}
	err = json.Unmarshal(bb, status)
	if err != nil {
		logrus.WithError(err).Error("error unmarshalling ncco status")
		return models.ChannelSessionStatusErrored, 0
	}

	switch status.Status {

	case "started", "ringing":
		return models.ChannelSessionStatusWired, 0

	case "answered":
		return models.ChannelSessionStatusInProgress, 0

	case "completed":
		duration, _ := strconv.Atoi(status.Duration)
		return models.ChannelSessionStatusCompleted, duration

	case "rejected", "busy", "unanswered", "timeout", "failed", "machine":
		return models.ChannelSessionStatusErrored, 0

	default:
		logrus.WithField("status", status.Status).Error("unknown call status in ncco callback")
		return models.ChannelSessionStatusWired, 0
	}
}

// ValidateRequestSignature validates the signature on the passed in request, returning an error if it is invaled
func (c *client) ValidateRequestSignature(r *http.Request) error {
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
func (c *client) WriteSessionResponse(session *models.Session, resumeURL string, r *http.Request, w http.ResponseWriter) error {
	// for errored sessions we should just output our error body
	if session.Status == models.SessionStatusErrored {
		return errors.Errorf("cannot write IVR response for errored session")
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

func (c *client) postRequest(client *http.Client, sendURL string, body interface{}) (*http.Response, error) {
	bb, err := json.Marshal(body)
	if err != nil {
		return nil, errors.Wrapf(err, "error json encoding request")
	}

	req, _ := http.NewRequest(http.MethodPost, sendURL, bytes.NewReader(bb))
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
	Action    string `json:"stream"`
	StreamURL string `json:",streamUrl"`
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
	Action      string   `json:"action"`
	EndOnKey    string   `json:"endOnKey"`
	Timeout     int      `json:"timeOut"`
	EventURL    []string `json:"eventUrl"`
	EventMethod string   `json:"eventMethod"`
}

func (c *client) responseForSprint(resumeURL string, w flows.Wait, es []flows.Event) (string, error) {
	actions := make([]interface{}, 0, 1)

	var wait interface{}

	if w != nil {
		msgWait, isMsgWait := w.(*waits.MsgWait)
		if !isMsgWait {
			return "", errors.Errorf("unable to use wait of type: %s in IVR call", w.Type())
		}

		switch hint := msgWait.Hint().(type) {
		case *hints.DigitsHint:
			eventURL := resumeURL + "&wait_type=gather"
			eventURL = eventURL + "&sig=" + url.QueryEscape(c.calculateSignature(eventURL))
			input := &Input{
				Action:       "input",
				Timeout:      inputTimeout,
				SubmitOnHash: true,
				EventURL:     []string{eventURL},
				EventMethod:  http.MethodPost,
			}
			if hint.Count != nil {
				input.MaxDigits = *hint.Count
			}
			wait = input

		case *hints.AudioHint:
			eventURL := resumeURL + "&wait_type=record"
			eventURL = eventURL + "&sig=" + url.QueryEscape(c.calculateSignature(eventURL))
			record := &Record{
				Action:      "record",
				EndOnKey:    "#",
				Timeout:     inputTimeout,
				EventURL:    []string{eventURL},
				EventMethod: http.MethodPost,
			}
			wait = record

		default:
			return "", errors.Errorf("unable to use wait in IVR call, unknow type: %s", msgWait.Hint().Type())
		}
	}

	_, isWaitInput := wait.(*Input)

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
						StreamURL: a.URL(),
					})
				}
			}
		}
	}

	if wait != nil {
		actions = append(actions, wait)
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
