package vonage

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
	"log/slog"
	"math/rand"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/buger/jsonparser"
	"github.com/golang-jwt/jwt"
	valkey "github.com/gomodule/redigo/redis"
	"github.com/nyaruka/gocommon/httpx"
	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/goflow/flows/routers/waits/hints"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/core/ivr"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/runner"
	"github.com/nyaruka/mailroom/runtime"
)

// IgnoreSignatures sets whether we ignore signatures (for unit tests)
var IgnoreSignatures = false

var callStatusMap = map[string]flows.DialStatus{
	"cancelled": flows.DialStatusFailed,
	"answered":  flows.DialStatusAnswered,
	"busy":      flows.DialStatusBusy,
	"timeout":   flows.DialStatusNoAnswer,
	"failed":    flows.DialStatusFailed,
	"rejected":  flows.DialStatusNoAnswer,
	"canceled":  flows.DialStatusFailed,
}

const (
	vonageChannelType = models.ChannelType("NX")

	gatherTimeout = 30
	recordTimeout = 600

	appIDConfig      = "nexmo_app_id"
	privateKeyConfig = "nexmo_app_private_key"

	statusFailed = "failed"
)

var indentMarshal = true

type service struct {
	httpClient *http.Client
	channel    *models.Channel
	callURL    string
	appID      string
	privateKey *rsa.PrivateKey
}

func init() {
	ivr.RegisterService(vonageChannelType, NewServiceFromChannel)
}

// NewServiceFromChannel creates a new Vonage IVR service for the passed in account and and auth token
func NewServiceFromChannel(httpClient *http.Client, channel *models.Channel) (ivr.Service, error) {
	appID := channel.Config().GetString(appIDConfig, "")
	key := channel.Config().GetString(privateKeyConfig, "")
	if appID == "" || key == "" {
		return nil, fmt.Errorf("missing %s or %s on channel config", appIDConfig, privateKeyConfig)
	}

	privateKey, err := jwt.ParseRSAPrivateKeyFromPEM([]byte(key))
	if err != nil {
		return nil, fmt.Errorf("error parsing private key: %w", err)
	}

	return &service{
		httpClient: httpClient,
		channel:    channel,
		callURL:    CallURL,
		appID:      appID,
		privateKey: privateKey,
	}, nil
}

func readBody(r *http.Request) ([]byte, error) {
	if r.Body == http.NoBody {
		return nil, nil
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, nil
	}
	r.Body = io.NopCloser(bytes.NewBuffer(body))
	return body, nil
}

func (s *service) CallIDForRequest(r *http.Request) (string, error) {
	body, err := readBody(r)
	if err != nil {
		return "", fmt.Errorf("error reading body from request: %w", err)
	}
	callID, err := jsonparser.GetString(body, "uuid")
	if err != nil {
		return "", fmt.Errorf("invalid json body")
	}

	if callID == "" {
		return "", fmt.Errorf("no uuid set on call")
	}
	return callID, nil
}

func (s *service) URNForRequest(r *http.Request) (urns.URN, error) {
	// get our recording url out
	body, err := readBody(r)
	if err != nil {
		return "", fmt.Errorf("error reading body from request: %w", err)
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
		return "", fmt.Errorf("invalid json body")
	}

	if urn == "" {
		return "", fmt.Errorf("no urn found in body")
	}
	return urns.ParsePhone("+"+urn, "", true, false)
}

func (s *service) DownloadMedia(url string) (*http.Response, error) {
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	token, err := s.generateToken()
	if err != nil {
		return nil, fmt.Errorf("error generating jwt token: %w", err)
	}

	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	return http.DefaultClient.Do(req)
}

func (s *service) CheckStartRequest(r *http.Request) models.CallError {
	return ""
}

func (s *service) PreprocessStatus(ctx context.Context, rt *runtime.Runtime, r *http.Request) ([]byte, error) {
	// parse out the call status, we are looking for a leg of one of our conferences ending in the "forward" case
	// get our recording url out
	body, _ := readBody(r)
	if len(body) == 0 {
		return nil, nil
	}

	// check the type of this status, we don't care about preprocessing "transfer" statuses
	nxType, err := jsonparser.GetString(body, "type")
	if err == jsonparser.MalformedJsonError {
		return nil, fmt.Errorf("invalid json body: %s: %w", body, err)
	}

	if nxType == "transfer" {
		return s.MakeEmptyResponseBody("ignoring conversation callback"), nil
	}

	// grab our uuid out
	legUUID, _ := jsonparser.GetString(body, "uuid")

	// and our status
	nxStatus, _ := jsonparser.GetString(body, "status")

	// if we are missing either, this is just a notification of the establishment of the conversation, ignore
	if legUUID == "" || nxStatus == "" {
		return nil, nil
	}

	// look up to see whether this is a call we need to track
	vc := rt.VK.Get()
	defer vc.Close()

	legKey := fmt.Sprintf("dial_%s", legUUID)
	dialContinue, err := valkey.String(vc.Do("GET", legKey))

	// no associated call, move on
	if err == valkey.ErrNil {
		return nil, nil
	}

	if err != nil {
		return nil, fmt.Errorf("error looking up leg uuid: %s: %w", legKey, err)
	}

	// transfer the call back to our handle with the dial wait type
	parts := strings.SplitN(dialContinue, ":", 2)
	callUUID, resumeURL := parts[0], parts[1]

	// we found an associated call, if the status is complete, have it continue, we call out to
	// vonage and hand it our flow to resume on to get the next NCCO
	if nxStatus == "completed" {
		slog.Debug("found completed call, trying to finish with call", "call_uuid", callUUID)
		statusKey := fmt.Sprintf("dial_status_%s", callUUID)
		status, err := valkey.String(vc.Do("GET", statusKey))
		if err == valkey.ErrNil {
			return nil, fmt.Errorf("unable to find call status for: %s", callUUID)
		}
		if err != nil {
			return nil, fmt.Errorf("error looking up call status for: %s: %w", callUUID, err)
		}

		// duration of the call is in our body
		duration, _ := jsonparser.GetString(body, "duration")

		resumeURL += "&dial_status=" + status
		resumeURL += "&dial_duration=" + duration
		resumeURL += "&sig=" + s.calculateSignature(resumeURL)

		nxBody := map[string]any{
			"action": "transfer",
			"destination": map[string]any{
				"type": "ncco",
				"url":  []string{resumeURL},
			},
		}
		trace, err := s.makeRequest(http.MethodPut, s.callURL+"/"+callUUID, nxBody)
		if err != nil {
			return nil, fmt.Errorf("error reconnecting flow for call: %s: %w", callUUID, err)
		}

		// vonage return 204 on successful updates
		if trace.Response.StatusCode != http.StatusNoContent {
			return nil, fmt.Errorf("error reconnecting flow for call: %s, received %d from vonage", callUUID, trace.Response.StatusCode)
		}

		return s.MakeEmptyResponseBody(fmt.Sprintf("reconnected call: %s to flow with dial status: %s", callUUID, status)), nil
	}

	// otherwise the call isn't over yet, instead stash away our status so we can use it to
	// determine if the call was answered, busy etc..
	status := callStatusMap[nxStatus]

	// only store away valid final states
	if status != "" {
		callKey := fmt.Sprintf("dial_status_%s", callUUID)
		_, err = vc.Do("SETEX", callKey, 300, status)
		if err != nil {
			return nil, fmt.Errorf("error inserting recording URL into valkey: %w", err)
		}

		slog.Debug("saved intermediary dial status for call", "callUUID", callUUID, "status", status, "key", callKey)

		return s.MakeEmptyResponseBody(fmt.Sprintf("updated status for call: %s to: %s", callUUID, status)), nil
	}

	return s.MakeEmptyResponseBody("ignoring non final status for tranfer leg"), nil
}

func (s *service) PreprocessResume(ctx context.Context, rt *runtime.Runtime, call *models.Call, r *http.Request) ([]byte, error) {
	// if this is a recording_url resume, grab that
	waitType := r.URL.Query().Get("wait_type")

	switch waitType {
	case "record":
		recordingUUID := r.URL.Query().Get("recording_uuid")
		if recordingUUID == "" {
			return nil, fmt.Errorf("record resume without recording_uuid")
		}

		vc := rt.VK.Get()
		defer vc.Close()

		recordingKey := fmt.Sprintf("recording_%s", recordingUUID)
		recordingURL, err := valkey.String(vc.Do("GET", recordingKey))
		if err != nil && err != valkey.ErrNil {
			return nil, fmt.Errorf("error getting recording url from valkey: %w", err)
		}

		// found a URL, stuff it in our request and move on
		if recordingURL != "" {
			r.URL.RawQuery = "&recording_url=" + url.QueryEscape(recordingURL)
			slog.Info("found recording URL", "recording_url", recordingURL)
			vc.Do("DEL", recordingKey)
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
		return json.MarshalIndent([]any{input}, "", "  ")

	case "recording_url":
		// this is our async callback for our recording URL, we stuff it in redis and return an empty response
		recordingUUID := r.URL.Query().Get("recording_uuid")
		if recordingUUID == "" {
			return nil, fmt.Errorf("recording_url resume without recording_uuid")
		}

		// get our recording url out
		body, err := io.ReadAll(r.Body)
		if err != nil {
			return nil, fmt.Errorf("error reading body from request: %w", err)
		}
		recordingURL, err := jsonparser.GetString(body, "recording_url")
		if err != nil {
			return nil, fmt.Errorf("invalid json body")
		}
		if recordingURL == "" {
			return nil, fmt.Errorf("no recording_url found in request")
		}

		// write it to redis
		vc := rt.VK.Get()
		defer vc.Close()

		recordingKey := fmt.Sprintf("recording_%s", recordingUUID)
		_, err = vc.Do("SETEX", recordingKey, 300, recordingURL)
		if err != nil {
			return nil, fmt.Errorf("error inserting recording URL into valkey: %w", err)
		}

		msgBody := map[string]string{
			"_message": fmt.Sprintf("inserted recording url: %s for uuid: %s", recordingURL, recordingUUID),
		}
		return json.MarshalIndent(msgBody, "", "  ")

	default:
		return nil, nil
	}
}

// RequestCall requests a new outgoing call for this service
func (s *service) RequestCall(number urns.URN, resumeURL string, statusURL string, machineDetection bool) (ivr.CallID, *httpx.Trace, error) {
	callR := &CallRequest{
		AnswerURL:    []string{resumeURL + "&sig=" + url.QueryEscape(s.calculateSignature(resumeURL))},
		AnswerMethod: http.MethodPost,

		EventURL:    []string{statusURL + "?sig=" + url.QueryEscape(s.calculateSignature(statusURL))},
		EventMethod: http.MethodPost,
	}

	if machineDetection {
		callR.MachineDetection = "hangup" // if an answering machine answers, just hangup
	}

	callR.To = append(callR.To, Phone{Type: "phone", Number: strings.TrimLeft(number.Path(), "+")})
	callR.From = Phone{Type: "phone", Number: strings.TrimLeft(s.channel.Address(), "+")}

	trace, err := s.makeRequest(http.MethodPost, s.callURL, callR)
	if err != nil {
		return ivr.NilCallID, trace, fmt.Errorf("error trying to start call: %w", err)
	}

	if trace.Response.StatusCode != http.StatusCreated {
		return ivr.NilCallID, trace, fmt.Errorf("received non 201 status for call start: %d", trace.Response.StatusCode)
	}

	// parse out our call sid
	call := &CallResponse{}
	err = json.Unmarshal(trace.ResponseBody, call)
	if err != nil || call.UUID == "" {
		return ivr.NilCallID, trace, fmt.Errorf("unable to read call uuid")
	}

	if call.Status == statusFailed {
		return ivr.NilCallID, trace, fmt.Errorf("call status returned as failed")
	}

	slog.Debug("requested call", "body", string(trace.ResponseBody), "status", trace.Response.StatusCode)

	return ivr.CallID(call.UUID), trace, nil
}

// HangupCall asks Vonage to hang up the call that is passed in
func (s *service) HangupCall(callID string) (*httpx.Trace, error) {
	hangupBody := map[string]string{"action": "hangup"}
	url := s.callURL + "/" + callID
	trace, err := s.makeRequest(http.MethodPut, url, hangupBody)
	if err != nil {
		return trace, fmt.Errorf("error trying to hangup call: %w", err)
	}

	if trace.Response.StatusCode != 204 {
		return trace, fmt.Errorf("received non 204 status for call hangup: %d", trace.Response.StatusCode)
	}
	return trace, nil
}

type NCCOInput struct {
	DTMF             string `json:"dtmf"`
	TimedOut         bool   `json:"timed_out"`
	UUID             string `json:"uuid"`
	ConversationUUID string `json:"conversation_uuid"`
	Timestamp        string `json:"timestamp"`
}

// ResumeForRequest returns the resume (input or dial) for the passed in request, if any
func (s *service) ResumeForRequest(r *http.Request) (ivr.Resume, error) {
	// this could be empty, in which case we return nothing at all
	empty := r.Form.Get("empty")
	if empty == "true" {
		return ivr.InputResume{}, nil
	}

	waitType := r.Form.Get("wait_type")

	// if this is an input, parse that
	if waitType == "gather" || waitType == "record" {
		// parse our input
		input := &NCCOInput{}
		bb, err := readBody(r)
		if err != nil {
			return nil, fmt.Errorf("error reading request body: %w", err)
		}

		err = json.Unmarshal(bb, input)
		if err != nil {
			return nil, fmt.Errorf("unable to parse ncco request: %w", err)
		}

		// otherwise grab the right field based on our wait type
		switch waitType {
		case "gather":
			// this could be a timeout, in which case we return nothing at all
			if input.TimedOut {
				return ivr.InputResume{}, nil
			}

			return ivr.InputResume{Input: input.DTMF}, nil

		case "record":
			recordingURL := r.URL.Query().Get("recording_url")
			if recordingURL == "" {
				return ivr.InputResume{}, nil
			}
			slog.Info("input found recording", "recording_url", recordingURL)
			return ivr.InputResume{Attachment: utils.Attachment("audio:" + recordingURL)}, nil

		default:
			return nil, fmt.Errorf("unknown wait_type: %s", waitType)
		}
	}

	// only remaining type should be dial
	if waitType != "dial" {
		return nil, fmt.Errorf("unknown wait_type: %s", waitType)
	}

	status := r.URL.Query().Get("dial_status")
	if status == "" {
		return nil, fmt.Errorf("unable to find dial_status in query url")
	}
	duration := 0
	d := r.URL.Query().Get("dial_duration")
	if d != "" {
		parsed, err := strconv.Atoi(d)
		if err != nil {
			return nil, fmt.Errorf("non-integer duration in query url")
		}
		duration = parsed
	}

	slog.Info("input found dial status and duration", "status", status, "duration", duration)
	return ivr.DialResume{Status: flows.DialStatus(status), Duration: duration}, nil
}

type StatusRequest struct {
	UUID     string `json:"uuid"`
	Status   string `json:"status"`
	Duration string `json:"duration"`
}

// StatusForRequest returns the current call status for the passed in status (and optional duration if known)
func (s *service) StatusForRequest(r *http.Request) (models.CallStatus, models.CallError, int) {
	// this is a resume, call is in progress, no need to look at the body
	if r.Form.Get("action") == "resume" {
		return models.CallStatusInProgress, "", 0
	}

	bb, err := readBody(r)
	if err != nil {
		slog.Error("error reading status request body", "error", err)
		return models.CallStatusErrored, models.CallErrorProvider, 0
	}

	status := &StatusRequest{}
	err = json.Unmarshal(bb, status)
	if err != nil {
		slog.Error("error unmarshalling ncco status", "error", err, "body", string(bb))
		return models.CallStatusErrored, models.CallErrorProvider, 0
	}

	// transfer status callbacks have no status, safe to ignore them
	if status.Status == "" {
		return models.CallStatusInProgress, "", 0
	}

	switch status.Status {

	case "started", "ringing":
		return models.CallStatusWired, "", 0

	case "answered":
		return models.CallStatusInProgress, "", 0

	case "completed":
		duration, _ := strconv.Atoi(status.Duration)
		return models.CallStatusCompleted, "", duration

	case "busy":
		return models.CallStatusErrored, models.CallErrorBusy, 0
	case "rejected", "unanswered", "timeout":
		return models.CallStatusErrored, models.CallErrorNoAnswer, 0
	case "machine":
		return models.CallStatusErrored, models.CallErrorMachine, 0
	case "failed":
		return models.CallStatusErrored, models.CallErrorProvider, 0

	default:
		slog.Error("unknown call status in ncco callback", "status", status.Status)
		return models.CallStatusFailed, models.CallErrorProvider, 0
	}
}

// ValidateRequestSignature validates the signature on the passed in request, returning an error if it is invaled
func (s *service) ValidateRequestSignature(r *http.Request) error {
	if IgnoreSignatures {
		return nil
	}

	// only validate handling calls, we can't verify others
	if !strings.HasSuffix(r.URL.Path, "handle") {
		return nil
	}

	actual := r.URL.Query().Get("sig")
	if actual == "" {
		return fmt.Errorf("missing request sig")
	}

	path := r.URL.RequestURI()
	proxyPath := r.Header.Get("X-Forwarded-Path")
	if proxyPath != "" {
		path = proxyPath
	}

	url := fmt.Sprintf("https://%s%s", r.Host, path)
	expected := s.calculateSignature(url)
	if expected != actual {
		return fmt.Errorf("mismatch in signatures for url: %s, actual: %s, expected: %s", url, actual, expected)
	}
	return nil
}

// WriteSessionResponse writes a NCCO response for the events in the passed in session
func (s *service) WriteSessionResponse(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, channel *models.Channel, scene *runner.Scene, number urns.URN, resumeURL string, r *http.Request, w http.ResponseWriter) error {
	// for errored sessions we should just output our error body
	if scene.Session.Status() == flows.SessionStatusFailed {
		return fmt.Errorf("cannot write IVR response for failed session")
	}

	// get our response
	response, err := s.responseForSprint(ctx, rt.VK, channel, scene.DBCall, resumeURL, scene.Sprint.Events())
	if err != nil {
		return fmt.Errorf("unable to build response for IVR call: %w", err)
	}

	w.Header().Set("Content-Type", "application/json")
	_, err = w.Write([]byte(response))
	if err != nil {
		return fmt.Errorf("error writing IVR response: %w", err)
	}

	return nil
}

func (s *service) WriteRejectResponse(w http.ResponseWriter) error {
	w.Header().Set("Content-Type", "application/json")
	_, err := w.Write(jsonx.MustMarshal([]any{Talk{
		Action: "talk",
		Text:   "This number is not accepting calls",
	}}))
	return err
}

// WriteErrorResponse writes an error / unavailable response
func (s *service) WriteErrorResponse(w http.ResponseWriter, err error) error {
	w.Header().Set("Content-Type", "application/json")
	_, err = w.Write(jsonx.MustMarshal([]any{Talk{
		Action: "talk",
		Text:   ivr.ErrorMessage,
		Error:  err.Error(),
	}}))
	return err
}

// WriteEmptyResponse writes an empty (but valid) response
func (s *service) WriteEmptyResponse(w http.ResponseWriter, msg string) error {
	w.Header().Set("Content-Type", "application/json")
	_, err := w.Write(s.MakeEmptyResponseBody(msg))
	return err
}

func (s *service) MakeEmptyResponseBody(msg string) []byte {
	return jsonx.MustMarshal(map[string]string{
		"_message": msg,
	})
}

func (s *service) makeRequest(method string, sendURL string, body any) (*httpx.Trace, error) {
	bb := jsonx.MustMarshal(body)
	req, _ := http.NewRequest(method, sendURL, bytes.NewReader(bb))
	token, err := s.generateToken()
	if err != nil {
		return nil, fmt.Errorf("error generating jwt token: %w", err)
	}

	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")

	return httpx.DoTrace(s.httpClient, req, nil, nil, -1)
}

// calculateSignature calculates a signature for the passed in URL
func (s *service) calculateSignature(u string) string {
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
	mac := hmac.New(sha1.New, []byte(s.appID))
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

func (s *service) generateToken() (string, error) {
	claims := jwtClaims{
		s.appID,
		jwt.StandardClaims{
			Id:       strconv.Itoa(rand.Int()),
			IssuedAt: time.Now().UTC().Unix(),
		},
	}
	token := jwt.NewWithClaims(jwt.GetSigningMethod("RS256"), claims)
	return token.SignedString(s.privateKey)
}

// NCCO building utilities

func (s *service) responseForSprint(ctx context.Context, rp *valkey.Pool, channel *models.Channel, call *models.Call, resumeURL string, es []flows.Event) (string, error) {
	actions := make([]any, 0, 1)
	waitActions := make([]any, 0, 1)

	var waitEvent flows.Event
	for _, e := range es {
		switch event := e.(type) {
		case *events.MsgWait, *events.DialWait:
			waitEvent = event
		}
	}

	if waitEvent != nil {
		switch wait := waitEvent.(type) {
		case *events.MsgWait:
			switch hint := wait.Hint.(type) {
			case *hints.Digits:
				eventURL := resumeURL + "&wait_type=gather"
				eventURL = eventURL + "&sig=" + url.QueryEscape(s.calculateSignature(eventURL))
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

			case *hints.Audio:
				// Vonage is goofy in that they do not synchronously send us recordings. Rather the move on in
				// the NCCO script immediately and then asynchronously call the event URL on the record URL
				// when the recording is ready.
				//
				// We deal with this by adding the record event with a status callback including a UUID
				// which we will store the recording url under when it is received. Meanwhile we put an input
				// with a 1 second timeout in the script that will get called / repeated until the UUID is
				// populated at which time we will actually continue.

				recordingUUID := string(uuids.NewV4())
				eventURL := resumeURL + "&wait_type=recording_url&recording_uuid=" + recordingUUID
				eventURL = eventURL + "&sig=" + url.QueryEscape(s.calculateSignature(eventURL))
				record := &Record{
					Action:       "record",
					EventURL:     []string{eventURL},
					EventMethod:  http.MethodPost,
					EndOnKey:     "#",
					Timeout:      recordTimeout,
					EndOnSilence: 5,
				}
				waitActions = append(waitActions, record)

				// Vonage is goofy in that they do not call our event URL upon gathering the recording but
				// instead move on. So we need to put in an input here as well
				eventURL = resumeURL + "&wait_type=record&recording_uuid=" + recordingUUID
				eventURL = eventURL + "&sig=" + url.QueryEscape(s.calculateSignature(eventURL))
				input := &Input{
					Action:       "input",
					Timeout:      1,
					SubmitOnHash: true,
					EventURL:     []string{eventURL},
					EventMethod:  http.MethodPost,
				}
				waitActions = append(waitActions, input)

			default:
				return "", fmt.Errorf("unable to use wait in IVR call, unknow hint type: %s", wait.Hint.Type())
			}

		case *events.DialWait:
			// Vonage handles forwards a bit differently. We have to create a new call to the forwarded number, then
			// join the current call with the call we are starting.
			//
			// See: https://developer.nexmo.com/use-cases/contact-center
			//
			// We then track the state of that call, restarting NCCO control of the original call when
			// the transfer has completed.
			conversationUUID := string(uuids.NewV4())
			connect := &Conversation{
				Action: "conversation",
				Name:   conversationUUID,
			}
			waitActions = append(waitActions, connect)

			// create our outbound cr with the same conversation UUID
			cr := CallRequest{
				From:         Phone{Type: "phone", Number: strings.TrimLeft(channel.Address(), "+")},
				To:           []Phone{{Type: "phone", Number: strings.TrimLeft(wait.URN.Path(), "+")}},
				NCCO:         []NCCO{{Action: "conversation", Name: conversationUUID}},
				RingingTimer: wait.DialLimitSeconds,
				LengthTimer:  wait.CallLimitSeconds,
			}

			trace, err := s.makeRequest(http.MethodPost, s.callURL, cr)
			slog.Debug("initiated new call for transfer", "trace", trace)
			if err != nil {
				return "", fmt.Errorf("error trying to start call: %w", err)
			}

			if trace.Response.StatusCode != http.StatusCreated {
				return "", fmt.Errorf("received non 200 status for call start: %d", trace.Response.StatusCode)
			}

			// we save away our call id, as we want to continue our original call when that is complete
			transferUUID, err := jsonparser.GetString(trace.ResponseBody, "uuid")
			if err != nil {
				return "", fmt.Errorf("error reading call id from transfer: %w", err)
			}

			// save away the tranfer id, connecting it to this connection
			vc := rp.Get()
			defer vc.Close()

			eventURL := resumeURL + "&wait_type=dial"
			vkKey := fmt.Sprintf("dial_%s", transferUUID)
			vkValue := fmt.Sprintf("%s:%s", call.ExternalID(), eventURL)
			_, err = vc.Do("SETEX", vkKey, 3600, vkValue)
			if err != nil {
				return "", fmt.Errorf("error inserting transfer ID into valkey: %w", err)
			}
			slog.Debug("saved away call", "transferUUID", transferUUID, "call", call.ExternalID(), "valkey_key", vkKey, "valkey_value", vkValue)
		}
	}

	isWaitInput := false
	if len(waitActions) > 0 {
		_, isWaitInput = waitActions[0].(*Input)
	}

	for _, e := range es {
		switch event := e.(type) {
		case *events.IVRCreated:
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

	actions = append(actions, waitActions...)

	var body []byte
	var err error
	if indentMarshal {
		body, err = json.MarshalIndent(actions, "", "  ")
	} else {
		body, err = json.Marshal(actions)
	}
	if err != nil {
		return "", fmt.Errorf("unable to marshal ncco body: %w", err)
	}

	return string(body), nil
}

func (s *service) RedactValues(ch *models.Channel) []string {
	return []string{ch.Config().GetString(privateKeyConfig, "")}
}
