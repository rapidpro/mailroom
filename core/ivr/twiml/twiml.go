package twiml

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/nyaruka/gocommon/httpx"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/goflow/flows/routers/waits"
	"github.com/nyaruka/goflow/flows/routers/waits/hints"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/core/ivr"
	"github.com/nyaruka/mailroom/core/models"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// BaseURL is our default base URL for TWIML channels (public for testing overriding)
var BaseURL = `https://api.twilio.com`

// IgnoreSignatures controls whether we ignore signatures (public for testing overriding)
var IgnoreSignatures = false

const (
	twilioChannelType     = models.ChannelType("T")
	twimlChannelType      = models.ChannelType("TW")
	signalWireChannelType = models.ChannelType("SW")

	callPath   = `/2010-04-01/Accounts/{AccountSID}/Calls.json`
	hangupPath = `/2010-04-01/Accounts/{AccountSID}/Calls/{SID}.json`

	signatureHeader = "X-Twilio-Signature"

	statusFailed = "failed"

	gatherTimeout = 30
	recordTimeout = 600

	accountSIDConfig = "account_sid"
	authTokenConfig  = "auth_token"

	sendURLConfig = "send_url"
	baseURLConfig = "base_url"

	errorBody = `<?xml version="1.0" encoding="UTF-8"?>
	<Response>
		<Say>An error was encountered. Goodbye.</Say>
		<Hangup></Hangup>
	</Response>
	`
)

var validLanguageCodes = map[string]bool{
	"da-DK": true,
	"de-DE": true,
	"en-AU": true,
	"en-CA": true,
	"en-GB": true,
	"en-IN": true,
	"en-US": true,
	"ca-ES": true,
	"es-ES": true,
	"es-MX": true,
	"fi-FI": true,
	"fr-CA": true,
	"fr-FR": true,
	"it-IT": true,
	"ja-JP": true,
	"ko-KR": true,
	"nb-NO": true,
	"nl-NL": true,
	"pl-PL": true,
	"pt-BR": true,
	"pt-PT": true,
	"ru-RU": true,
	"sv-SE": true,
	"zh-CN": true,
	"zh-HK": true,
	"zh-TW": true,
}

var indentMarshal = true

type client struct {
	httpClient   *http.Client
	channel      *models.Channel
	baseURL      string
	accountSID   string
	authToken    string
	validateSigs bool
}

func init() {
	ivr.RegisterClientType(twimlChannelType, NewClientFromChannel)
	ivr.RegisterClientType(twilioChannelType, NewClientFromChannel)
	ivr.RegisterClientType(signalWireChannelType, NewClientFromChannel)
}

// NewClientFromChannel creates a new Twilio IVR client for the passed in account and and auth token
func NewClientFromChannel(httpClient *http.Client, channel *models.Channel) (ivr.Client, error) {
	accountSID := channel.ConfigValue(accountSIDConfig, "")
	authToken := channel.ConfigValue(authTokenConfig, "")
	if accountSID == "" || authToken == "" {
		return nil, errors.Errorf("missing auth_token or account_sid on channel config: %v for channel: %s", channel.Config(), channel.UUID())
	}
	baseURL := channel.ConfigValue(baseURLConfig, channel.ConfigValue(sendURLConfig, BaseURL))

	return &client{
		httpClient:   httpClient,
		channel:      channel,
		baseURL:      baseURL,
		accountSID:   accountSID,
		authToken:    authToken,
		validateSigs: channel.Type() != signalWireChannelType,
	}, nil
}

// NewClient creates a new Twilio IVR client for the passed in account and and auth token
func NewClient(httpClient *http.Client, accountSID string, authToken string) ivr.Client {
	return &client{
		httpClient: httpClient,
		baseURL:    BaseURL,
		accountSID: accountSID,
		authToken:  authToken,
	}
}

func (c *client) DownloadMedia(url string) (*http.Response, error) {
	return http.Get(url)
}

func (c *client) PreprocessResume(ctx context.Context, db *sqlx.DB, rp *redis.Pool, conn *models.ChannelConnection, r *http.Request) ([]byte, error) {
	return nil, nil
}

func (c *client) CallIDForRequest(r *http.Request) (string, error) {
	r.ParseForm()
	callID := r.Form.Get("CallSid")
	if callID == "" {
		return "", errors.Errorf("no CallSid parameter found in URL: %s", r.URL)
	}
	return callID, nil
}

func (c *client) URNForRequest(r *http.Request) (urns.URN, error) {
	r.ParseForm()
	tel := r.Form.Get("Caller")
	if tel == "" {
		return "", errors.Errorf("no Caller parameter found in URL: %s", r.URL)
	}
	return urns.NewTelURNForCountry(tel, "")
}

// CallResponse is our struct for a Twilio call response
type CallResponse struct {
	SID    string `json:"sid"`
	Status string `json:"status"`
}

// RequestCall causes this client to request a new outgoing call for this provider
func (c *client) RequestCall(number urns.URN, callbackURL string, statusURL string) (ivr.CallID, *httpx.Trace, error) {
	form := url.Values{}
	form.Set("To", number.Path())
	form.Set("From", c.channel.Address())
	form.Set("Url", callbackURL)
	form.Set("StatusCallback", statusURL)

	sendURL := c.baseURL + strings.Replace(callPath, "{AccountSID}", c.accountSID, -1)

	trace, err := c.postRequest(sendURL, form)
	if err != nil {
		return ivr.NilCallID, trace, errors.Wrapf(err, "error trying to start call")
	}

	if trace.Response.StatusCode != 201 {
		return ivr.NilCallID, trace, errors.Errorf("received non 201 status for call start: %d", trace.Response.StatusCode)
	}

	// parse out our call sid
	call := &CallResponse{}
	err = json.Unmarshal(trace.ResponseBody, call)
	if err != nil || call.SID == "" {
		return ivr.NilCallID, trace, errors.Errorf("unable to read call id")
	}

	if call.Status == statusFailed {
		return ivr.NilCallID, trace, errors.Errorf("call status returned as failed")
	}

	return ivr.CallID(call.SID), trace, nil
}

// HangupCall asks Twilio to hang up the call that is passed in
func (c *client) HangupCall(callID string) (*httpx.Trace, error) {
	form := url.Values{}
	form.Set("Status", "completed")

	sendURL := c.baseURL + strings.Replace(hangupPath, "{AccountSID}", c.accountSID, -1)
	sendURL = strings.Replace(sendURL, "{SID}", callID, -1)

	trace, err := c.postRequest(sendURL, form)
	if err != nil {
		return trace, errors.Wrapf(err, "error trying to hangup call")
	}

	if trace.Response.StatusCode != 200 {
		return trace, errors.Errorf("received non 204 trying to hang up call: %d", trace.Response.StatusCode)
	}

	return trace, nil
}

// InputForRequest returns the input for the passed in request, if any
func (c *client) InputForRequest(r *http.Request) (string, utils.Attachment, error) {
	// this could be a timeout, in which case we return nothing at all
	timeout := r.Form.Get("timeout")
	if timeout == "true" {
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
		return r.Form.Get("Digits"), utils.Attachment(""), nil
	case "record":
		url := r.Form.Get("RecordingUrl")
		if url == "" {
			return "", ivr.NilAttachment, nil
		}
		return "", utils.Attachment("audio/mp3:" + url + ".mp3"), nil
	default:
		return "", ivr.NilAttachment, errors.Errorf("unknown wait_type: %s", waitType)
	}
}

// StatusForRequest returns the current call status for the passed in status (and optional duration if known)
func (c *client) StatusForRequest(r *http.Request) (models.ConnectionStatus, int) {
	status := r.Form.Get("CallStatus")
	switch status {

	case "queued", "ringing":
		return models.ConnectionStatusWired, 0

	case "in-progress", "initiated":
		return models.ConnectionStatusInProgress, 0

	case "completed":
		duration, _ := strconv.Atoi(r.Form.Get("CallDuration"))
		return models.ConnectionStatusCompleted, duration

	case "busy", "no-answer", "canceled", "failed":
		return models.ConnectionStatusErrored, 0

	default:
		logrus.WithField("call_status", status).Error("unknown call status in ivr callback")
		return models.ConnectionStatusFailed, 0
	}
}

// ValidateRequestSignature validates the signature on the passed in request, returning an error if it is invaled
func (c *client) ValidateRequestSignature(r *http.Request) error {
	// shortcut for testing
	if IgnoreSignatures || !c.validateSigs {
		return nil
	}

	actual := r.Header.Get(signatureHeader)
	if actual == "" {
		return errors.Errorf("missing request signature header")
	}

	r.ParseForm()

	path := r.URL.RequestURI()
	proxyPath := r.Header.Get("X-Forwarded-Path")
	if proxyPath != "" {
		path = proxyPath
	}

	url := fmt.Sprintf("https://%s%s", r.Host, path)
	expected, err := twCalculateSignature(url, r.PostForm, c.authToken)
	if err != nil {
		return errors.Wrapf(err, "error calculating signature")
	}

	// compare signatures in way that isn't sensitive to a timing attack
	if !hmac.Equal(expected, []byte(actual)) {
		return errors.Errorf("invalid request signature: %s", actual)
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
	response, err := responseForSprint(number, resumeURL, session.Wait(), sprint.Events())
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
	r := &Response{Message: strings.Replace(err.Error(), "--", "__", -1)}
	r.Commands = append(r.Commands, Say{Text: ivr.ErrorMessage})
	r.Commands = append(r.Commands, Hangup{})

	body, err := xml.Marshal(r)
	if err != nil {
		return err
	}
	_, err = w.Write([]byte(xml.Header + string(body)))
	return err
}

// WriteEmptyResponse writes an empty (but valid) response
func (c *client) WriteEmptyResponse(w http.ResponseWriter, msg string) error {
	r := &Response{Message: strings.Replace(msg, "--", "__", -1)}

	body, err := xml.Marshal(r)
	if err != nil {
		return err
	}
	_, err = w.Write([]byte(xml.Header + string(body)))
	return err
}

func (c *client) postRequest(sendURL string, form url.Values) (*httpx.Trace, error) {
	req, _ := http.NewRequest(http.MethodPost, sendURL, strings.NewReader(form.Encode()))
	req.SetBasicAuth(c.accountSID, c.authToken)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	return httpx.DoTrace(c.httpClient, req, nil, nil, -1)
}

// see https://www.twilio.com/docs/api/security
func twCalculateSignature(url string, form url.Values, authToken string) ([]byte, error) {
	var buffer bytes.Buffer
	buffer.WriteString(url)

	keys := make(sort.StringSlice, 0, len(form))
	for k := range form {
		keys = append(keys, k)
	}
	keys.Sort()

	for _, k := range keys {
		buffer.WriteString(k)
		for _, v := range form[k] {
			buffer.WriteString(v)
		}
	}

	// hash with SHA1
	mac := hmac.New(sha1.New, []byte(authToken))
	mac.Write(buffer.Bytes())
	hash := mac.Sum(nil)

	// encode with Base64
	encoded := make([]byte, base64.StdEncoding.EncodedLen(len(hash)))
	base64.StdEncoding.Encode(encoded, hash)

	return encoded, nil
}

// TWIML building utilities

type Say struct {
	XMLName  string `xml:"Say"`
	Text     string `xml:",chardata"`
	Language string `xml:"language,attr,omitempty"`
}

type Play struct {
	XMLName string `xml:"Play"`
	URL     string `xml:",chardata"`
}

type Hangup struct {
	XMLName string `xml:"Hangup"`
}

type Redirect struct {
	XMLName string `xml:"Redirect"`
	URL     string `xml:",chardata"`
}

type Gather struct {
	XMLName     string        `xml:"Gather"`
	NumDigits   int           `xml:"numDigits,attr,omitempty"`
	FinishOnKey string        `xml:"finishOnKey,attr,omitempty"`
	Timeout     int           `xml:"timeout,attr,omitempty"`
	Action      string        `xml:"action,attr,omitempty"`
	Commands    []interface{} `xml:",innerxml"`
}

type Record struct {
	XMLName   string `xml:"Record"`
	Action    string `xml:"action,attr,omitempty"`
	MaxLength int    `xml:"maxLength,attr,omitempty"`
}

type Response struct {
	XMLName  string        `xml:"Response"`
	Message  string        `xml:",comment"`
	Gather   *Gather       `xml:"Gather"`
	Commands []interface{} `xml:",innerxml"`
}

func responseForSprint(number urns.URN, resumeURL string, w flows.ActivatedWait, es []flows.Event) (string, error) {
	r := &Response{}
	commands := make([]interface{}, 0)

	for _, e := range es {
		switch event := e.(type) {
		case *events.IVRCreatedEvent:
			if len(event.Msg.Attachments()) == 0 {
				country := envs.DeriveCountryFromTel(number.Path())
				locale := envs.NewLocale(event.Msg.TextLanguage, country)
				languageCode := locale.ToISO639_2()

				if _, valid := validLanguageCodes[languageCode]; !valid {
					languageCode = ""
				}
				commands = append(commands, Say{Text: event.Msg.Text(), Language: languageCode})
			} else {
				for _, a := range event.Msg.Attachments() {
					a = models.NormalizeAttachment(a)
					commands = append(commands, Play{URL: a.URL()})
				}
			}
		}
	}

	if w != nil {
		msgWait, isMsgWait := w.(*waits.ActivatedMsgWait)
		if !isMsgWait {
			return "", errors.Errorf("unable to use wait of type: %s in IVR call", w.Type())
		}

		switch hint := msgWait.Hint().(type) {
		case *hints.DigitsHint:
			resumeURL = resumeURL + "&wait_type=gather"
			gather := &Gather{
				Action:   resumeURL,
				Commands: commands,
				Timeout:  gatherTimeout,
			}
			if hint.Count != nil {
				gather.NumDigits = *hint.Count
			}
			gather.FinishOnKey = hint.TerminatedBy
			r.Gather = gather
			r.Commands = append(r.Commands, Redirect{URL: resumeURL + "&timeout=true"})

		case *hints.AudioHint:
			resumeURL = resumeURL + "&wait_type=record"
			commands = append(commands, Record{Action: resumeURL, MaxLength: recordTimeout})
			commands = append(commands, Redirect{URL: resumeURL + "&empty=true"})
			r.Commands = commands

		default:
			return "", errors.Errorf("unable to use wait in IVR call, unknow type: %s", msgWait.Hint().Type())
		}
	} else {
		// no wait? call is over, hang up
		commands = append(commands, Hangup{})
		r.Commands = commands
	}

	var body []byte
	var err error
	if indentMarshal {
		body, err = xml.MarshalIndent(r, "", "  ")
	} else {
		body, err = xml.Marshal(r)
	}
	if err != nil {
		return "", errors.Wrap(err, "unable to marshal twiml body")
	}

	return xml.Header + string(body), nil
}
