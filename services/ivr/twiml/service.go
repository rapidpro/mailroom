package twiml

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/nyaruka/gocommon/httpx"
	"github.com/nyaruka/gocommon/i18n"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/goflow/flows/routers/waits/hints"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/core/ivr"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/pkg/errors"
)

// IgnoreSignatures controls whether we ignore signatures (public for testing overriding)
var IgnoreSignatures = false

var dialStatusMap = map[string]flows.DialStatus{
	"completed": flows.DialStatusAnswered,
	"answered":  flows.DialStatusAnswered,
	"busy":      flows.DialStatusBusy,
	"no-answer": flows.DialStatusNoAnswer,
	"failed":    flows.DialStatusFailed,
	"canceled":  flows.DialStatusFailed,
}

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
)

// https://www.twilio.com/docs/voice/twiml/say
var supportedSayLanguages = i18n.NewBCP47Matcher(
	"af-ZA",
	"ar-AE",
	"ar-XA",
	"arb",
	"bg-BG",
	"ca-ES",
	"cmn-CN",
	"cmn-TW",
	"cs-CZ",
	"cy-GB",
	"da-DK",
	"de-AT",
	"de-DE",
	"el-GR",
	"en-AU",
	"en-GB",
	"en-IN",
	"en-NZ",
	"en-US",
	"en-ZA",
	"es-ES",
	"es-MX",
	"es-US",
	"eu-ES",
	"fi-FI",
	"fil-PH",
	"fr-CA",
	"fr-FR",
	"gl-ES",
	"he-IL",
	"hi-IN",
	"hu-HU",
	"id-ID",
	"is-IS",
	"it-IT",
	"ja-JP",
	"ko-KR",
	"lt-LT",
	"lv-LV",
	"ml-IN",
	"mr-IN",
	"ms-MY",
	"nb-NO",
	"nl-BE",
	"nl-NL",
	"pa-IN",
	"pl-PL",
	"pt-BR",
	"pt-PT",
	"ro-RO",
	"ru-RU",
	"sk-SK",
	"sr-RS",
	"sv-SE",
	"ta-IN",
	"te-IN",
	"th-TH",
	"tr-TR",
	"uk-UA",
	"vi-VN",
	"yue-CN",
	"yue-HK",
)

type service struct {
	httpClient   *http.Client
	channel      *models.Channel
	baseURL      string
	accountSID   string
	authToken    string
	validateSigs bool
}

func init() {
	ivr.RegisterServiceType(twimlChannelType, NewServiceFromChannel)
	ivr.RegisterServiceType(twilioChannelType, NewServiceFromChannel)
	ivr.RegisterServiceType(signalWireChannelType, NewServiceFromChannel)
}

// NewServiceFromChannel creates a new Twilio IVR service for the passed in account and and auth token
func NewServiceFromChannel(httpClient *http.Client, channel *models.Channel) (ivr.Service, error) {
	accountSID := channel.ConfigValue(accountSIDConfig, "")
	authToken := channel.ConfigValue(authTokenConfig, "")
	if accountSID == "" || authToken == "" {
		return nil, errors.Errorf("missing auth_token or account_sid on channel config: %v for channel: %s", channel.Config(), channel.UUID())
	}
	baseURL := channel.ConfigValue(baseURLConfig, channel.ConfigValue(sendURLConfig, BaseURL))

	return &service{
		httpClient:   httpClient,
		channel:      channel,
		baseURL:      baseURL,
		accountSID:   accountSID,
		authToken:    authToken,
		validateSigs: channel.Type() != signalWireChannelType,
	}, nil
}

// NewService creates a new Twilio IVR service for the passed in account and and auth token
func NewService(httpClient *http.Client, accountSID string, authToken string) ivr.Service {
	return &service{
		httpClient: httpClient,
		baseURL:    BaseURL,
		accountSID: accountSID,
		authToken:  authToken,
	}
}

func (s *service) DownloadMedia(url string) (*http.Response, error) {
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	req.SetBasicAuth(s.accountSID, s.authToken)
	return http.DefaultClient.Do(req)
}

func (s *service) CheckStartRequest(r *http.Request) models.CallError {
	r.ParseForm()
	answeredBy := r.Form.Get("AnsweredBy")
	if answeredBy == "machine_start" || answeredBy == "fax" {
		return models.CallErrorMachine
	}
	return ""
}

func (s *service) PreprocessStatus(ctx context.Context, rt *runtime.Runtime, r *http.Request) ([]byte, error) {
	return nil, nil
}

func (s *service) PreprocessResume(ctx context.Context, rt *runtime.Runtime, call *models.Call, r *http.Request) ([]byte, error) {
	return nil, nil
}

func (s *service) CallIDForRequest(r *http.Request) (string, error) {
	r.ParseForm()
	callID := r.Form.Get("CallSid")
	if callID == "" {
		return "", errors.Errorf("no CallSid parameter found in URL: %s", r.URL)
	}
	return callID, nil
}

func (s *service) URNForRequest(r *http.Request) (urns.URN, error) {
	r.ParseForm()
	tel := r.Form.Get("Caller")
	if tel == "" {
		tel = r.Form.Get("From")
	}
	if tel == "" {
		return "", errors.New("no Caller or From parameter found in request")
	}
	return urns.NewTelURNForCountry(tel, "")
}

// CallResponse is our struct for a Twilio call response
type CallResponse struct {
	SID    string `json:"sid" validate:"required"`
	Status string `json:"status"`
}

// RequestCall causes this client to request a new outgoing call for this provider
func (s *service) RequestCall(number urns.URN, callbackURL string, statusURL string, machineDetection bool) (ivr.CallID, *httpx.Trace, error) {
	form := url.Values{}
	form.Set("To", number.Path())
	form.Set("From", s.channel.Address())
	form.Set("Url", callbackURL)
	form.Set("StatusCallback", statusURL)

	if machineDetection {
		form.Set("MachineDetection", "Enable")
	}

	sendURL := s.baseURL + strings.Replace(callPath, "{AccountSID}", s.accountSID, -1)

	trace, err := s.postRequest(sendURL, form)
	if err != nil {
		return ivr.NilCallID, trace, errors.Wrapf(err, "error trying to start call")
	}

	if trace.Response.StatusCode != 201 {
		return ivr.NilCallID, trace, errors.Errorf("received non 201 status for call start: %d", trace.Response.StatusCode)
	}

	// parse the response from Twilio
	call := &CallResponse{}
	if err := utils.UnmarshalAndValidate(trace.ResponseBody, call); err != nil {
		return ivr.NilCallID, trace, errors.Wrap(err, "unable parse Twilio response")
	}
	if call.Status == statusFailed {
		return ivr.NilCallID, trace, errors.Errorf("call status returned as failed")
	}

	return ivr.CallID(call.SID), trace, nil
}

// HangupCall asks Twilio to hang up the call that is passed in
func (s *service) HangupCall(callID string) (*httpx.Trace, error) {
	form := url.Values{}
	form.Set("Status", "completed")

	sendURL := s.baseURL + strings.Replace(hangupPath, "{AccountSID}", s.accountSID, -1)
	sendURL = strings.Replace(sendURL, "{SID}", callID, -1)

	trace, err := s.postRequest(sendURL, form)
	if err != nil {
		return trace, errors.Wrapf(err, "error trying to hangup call")
	}

	if trace.Response.StatusCode != 200 {
		return trace, errors.Errorf("received non 204 trying to hang up call: %d", trace.Response.StatusCode)
	}

	return trace, nil
}

// InputForRequest returns the input for the passed in request, if any
func (s *service) ResumeForRequest(r *http.Request) (ivr.Resume, error) {
	// this could be a timeout, in which case we return an empty input
	timeout := r.Form.Get("timeout")
	if timeout == "true" {
		return ivr.InputResume{}, nil
	}

	// this could be empty, in which case we return an empty input
	empty := r.Form.Get("empty")
	if empty == "true" {
		return ivr.InputResume{}, nil
	}

	// otherwise grab the right field based on our wait type
	waitType := r.Form.Get("wait_type")
	switch waitType {
	case "gather":
		return ivr.InputResume{Input: r.Form.Get("Digits")}, nil

	case "record":
		url := r.Form.Get("RecordingUrl")
		if url == "" {
			return ivr.InputResume{}, nil
		}
		return ivr.InputResume{Attachment: utils.Attachment("audio/mp3:" + url + ".mp3")}, nil

	case "dial":
		twStatus := r.Form.Get("DialCallStatus")
		status := dialStatusMap[twStatus]
		if status == "" {
			return nil, errors.Errorf("unknown Twilio DialCallStatus in callback: %s", twStatus)
		}
		durationStr := r.Form.Get("DialCallDuration")
		var duration int64
		if durationStr != "" {
			var err error
			duration, err = strconv.ParseInt(durationStr, 10, 64)
			if err != nil {
				return nil, errors.Errorf("invalid value for DialCallDuration: %s", durationStr)
			}
		}

		return ivr.DialResume{Status: status, Duration: int(duration)}, nil

	default:
		return nil, errors.Errorf("unknown wait_type: %s", waitType)
	}
}

// StatusForRequest returns the call status for the passed in request, and if it's an error the reason,
// and if available, the current call duration
func (s *service) StatusForRequest(r *http.Request) (models.CallStatus, models.CallError, int) {
	status := r.Form.Get("CallStatus")
	switch status {

	case "queued", "ringing":
		return models.CallStatusWired, "", 0
	case "in-progress", "initiated":
		return models.CallStatusInProgress, "", 0
	case "completed":
		duration, _ := strconv.Atoi(r.Form.Get("CallDuration"))
		return models.CallStatusCompleted, "", duration

	case "busy":
		return models.CallStatusErrored, models.CallErrorBusy, 0
	case "no-answer":
		return models.CallStatusErrored, models.CallErrorNoAnswer, 0
	case "canceled", "failed":
		return models.CallStatusErrored, models.CallErrorProvider, 0

	default:
		slog.Error("unknown call status in status callback", "call_status", status)
		return models.CallStatusFailed, models.CallErrorProvider, 0
	}
}

// ValidateRequestSignature validates the signature on the passed in request, returning an error if it is invaled
func (s *service) ValidateRequestSignature(r *http.Request) error {
	// shortcut for testing
	if IgnoreSignatures || !s.validateSigs {
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
	expected, err := twCalculateSignature(url, r.PostForm, s.authToken)
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
func (s *service) WriteSessionResponse(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, channel *models.Channel, call *models.Call, session *models.Session, number urns.URN, resumeURL string, r *http.Request, w http.ResponseWriter) error {
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
	response, err := ResponseForSprint(rt, oa.Env(), number, resumeURL, sprint.Events(), true)
	if err != nil {
		return errors.Wrap(err, "unable to build response for IVR call")
	}

	_, err = w.Write([]byte(response))
	if err != nil {
		return errors.Wrap(err, "error writing IVR response")
	}

	return nil
}

func (s *service) WriteRejectResponse(w http.ResponseWriter) error {
	return s.writeResponse(w, &Response{
		Commands: []any{Reject{}},
	})
}

// WriteErrorResponse writes an error / unavailable response
func (s *service) WriteErrorResponse(w http.ResponseWriter, err error) error {
	return s.writeResponse(w, &Response{
		Message: strings.Replace(err.Error(), "--", "__", -1),
		Commands: []any{
			Say{Text: ivr.ErrorMessage},
			Hangup{},
		},
	})
}

// WriteEmptyResponse writes an empty (but valid) response
func (s *service) WriteEmptyResponse(w http.ResponseWriter, msg string) error {
	return s.writeResponse(w, &Response{
		Message: strings.Replace(msg, "--", "__", -1),
	})
}

func (s *service) writeResponse(w http.ResponseWriter, resp *Response) error {
	marshalled, err := xml.Marshal(resp)
	if err != nil {
		return err
	}
	w.Write([]byte(xml.Header))
	_, err = w.Write(marshalled)
	return err
}

func (s *service) postRequest(sendURL string, form url.Values) (*httpx.Trace, error) {
	req, _ := http.NewRequest(http.MethodPost, sendURL, strings.NewReader(form.Encode()))
	req.SetBasicAuth(s.accountSID, s.authToken)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	return httpx.DoTrace(s.httpClient, req, nil, nil, -1)
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

func ResponseForSprint(rt *runtime.Runtime, env envs.Environment, urn urns.URN, resumeURL string, es []flows.Event, indent bool) (string, error) {
	r := &Response{}
	commands := make([]any, 0)
	hasWait := false

	for _, e := range es {
		switch event := e.(type) {
		case *events.IVRCreatedEvent:
			if len(event.Msg.Attachments()) == 0 {
				var locales []i18n.Locale
				if event.Msg.Locale() != "" {
					locales = append(locales, event.Msg.Locale())
				}
				locales = append(locales, env.DefaultLocale())
				lang := supportedSayLanguages.ForLocales(locales...)

				commands = append(commands, &Say{Text: event.Msg.Text(), Language: lang})
			} else {
				for _, a := range event.Msg.Attachments() {
					a = models.NormalizeAttachment(rt.Config, a)
					commands = append(commands, Play{URL: a.URL()})
				}
			}

		case *events.MsgWaitEvent:
			hasWait = true
			switch hint := event.Hint.(type) {
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
				return "", errors.Errorf("unable to use hint in IVR call, unknown type: %s", event.Hint.Type())
			}

		case *events.DialWaitEvent:
			hasWait = true
			dial := Dial{Action: resumeURL + "&wait_type=dial", Number: event.URN.Path(), Timeout: event.DialLimitSeconds, TimeLimit: event.CallLimitSeconds}
			commands = append(commands, dial)
			r.Commands = commands
		}
	}

	if !hasWait {
		// no wait? call is over, hang up
		commands = append(commands, Hangup{})
		r.Commands = commands
	}

	var body []byte
	var err error
	if indent {
		body, err = xml.MarshalIndent(r, "", "  ")
	} else {
		body, err = xml.Marshal(r)
	}
	if err != nil {
		return "", errors.Wrap(err, "unable to marshal twiml body")
	}

	return xml.Header + string(body), nil
}

func (s *service) RedactValues(ch *models.Channel) []string {
	return []string{
		httpx.BasicAuth(ch.ConfigValue(accountSIDConfig, ""), ch.ConfigValue(authTokenConfig, "")),
		ch.ConfigValue(authTokenConfig, ""),
	}
}
