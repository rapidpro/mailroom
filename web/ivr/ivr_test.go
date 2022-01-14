package ivr

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/nyaruka/gocommon/dbutil/assertdb"
	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/goflow/test"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/queue"
	"github.com/nyaruka/mailroom/core/tasks/starts"
	"github.com/nyaruka/mailroom/web"
	"github.com/sirupsen/logrus"

	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/nyaruka/mailroom/core/handlers"
	ivr_tasks "github.com/nyaruka/mailroom/core/tasks/ivr"
	"github.com/nyaruka/mailroom/services/ivr/twiml"
	"github.com/nyaruka/mailroom/services/ivr/vonage"
)

// mocks the Twilio API
func mockTwilioHandler(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	logrus.WithField("method", r.Method).WithField("url", r.URL.String()).WithField("form", r.Form).Info("test server called")
	if strings.HasSuffix(r.URL.String(), "Calls.json") {
		to := r.Form.Get("To")
		if to == "+16055741111" {
			w.WriteHeader(http.StatusCreated)
			w.Write([]byte(`{"sid": "Call1"}`))
		} else if to == "+16055742222" {
			w.WriteHeader(http.StatusCreated)
			w.Write([]byte(`{"sid": "Call2"}`))
		} else if to == "+16055743333" {
			w.WriteHeader(http.StatusCreated)
			w.Write([]byte(`{"sid": "Call3"}`))
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}
	if strings.HasSuffix(r.URL.String(), "recording.mp3") {
		w.WriteHeader(http.StatusOK)
	}
}

func TestTwilioIVR(t *testing.T) {
	ctx, rt, db, rp := testsuite.Get()
	rc := rp.Get()
	defer rc.Close()

	defer testsuite.Reset(testsuite.ResetAll)

	// start test server
	ts := httptest.NewServer(http.HandlerFunc(mockTwilioHandler))
	defer ts.Close()

	twiml.BaseURL = ts.URL
	twiml.IgnoreSignatures = true

	wg := &sync.WaitGroup{}
	server := web.NewServer(ctx, rt, wg)
	server.Start()
	defer server.Stop()

	// add auth tokens
	db.MustExec(`UPDATE channels_channel SET config = '{"auth_token": "token", "account_sid": "sid", "callback_domain": "localhost:8090", "machine_detection": true}' WHERE id = $1`, testdata.TwilioChannel.ID)

	// create a flow start for cathy bob, and george
	parentSummary := json.RawMessage(`{
		"flow": {"name": "IVR Flow", "uuid": "2f81d0ea-4d75-4843-9371-3f7465311cce"}, 
		"uuid": "8bc73097-ac57-47fb-82e5-184f8ec6dbef", 
		"status": "active", 
		"contact": {
			"id": 10000, 
			"name": "Cathy", 
			"urns": ["tel:+16055741111?id=10000&priority=50"], 
			"uuid": "6393abc0-283d-4c9b-a1b3-641a035c34bf", 
			"fields": {"gender": {"text": "F"}}, 
			"groups": [{"name": "Doctors", "uuid": "c153e265-f7c9-4539-9dbc-9b358714b638"}], 
			"timezone": "America/Los_Angeles", 
			"created_on": "2019-07-23T09:35:01.439614-07:00"
		}, 
		"results": {}
	}`)
	start := models.NewFlowStart(testdata.Org1.ID, models.StartTypeTrigger, models.FlowTypeVoice, testdata.IVRFlow.ID, models.DoRestartParticipants, models.DoIncludeActive).
		WithContactIDs([]models.ContactID{testdata.Cathy.ID, testdata.Bob.ID, testdata.George.ID}).
		WithParentSummary(parentSummary)

	err := models.InsertFlowStarts(ctx, db, []*models.FlowStart{start})
	require.NoError(t, err)

	// call our master starter
	err = starts.CreateFlowBatches(ctx, rt, start)
	require.NoError(t, err)

	// start our task
	task, err := queue.PopNextTask(rc, queue.BatchQueue)
	require.NoError(t, err)
	batch := &models.FlowStartBatch{}
	jsonx.MustUnmarshal(task.Task, batch)

	// request our calls to start
	err = ivr_tasks.HandleFlowStartBatch(ctx, rt, batch)
	require.NoError(t, err)

	// check our 3 contacts have 3 wired calls
	assertdb.Query(t, db, `SELECT COUNT(*) FROM channels_channelconnection WHERE contact_id = $1 AND status = $2 AND external_id = $3`,
		testdata.Cathy.ID, models.ConnectionStatusWired, "Call1").Returns(1)
	assertdb.Query(t, db, `SELECT COUNT(*) FROM channels_channelconnection WHERE contact_id = $1 AND status = $2 AND external_id = $3`,
		testdata.Bob.ID, models.ConnectionStatusWired, "Call2").Returns(1)
	assertdb.Query(t, db, `SELECT COUNT(*) FROM channels_channelconnection WHERE contact_id = $1 AND status = $2 AND external_id = $3`,
		testdata.George.ID, models.ConnectionStatusWired, "Call3").Returns(1)

	tcs := []struct {
		label              string
		url                string
		form               url.Values
		expectedStatus     int
		expectedResponse   string
		expectedContains   []string
		expectedConnStatus map[string]string
	}{
		{
			label:              "handle start on wired connection",
			url:                fmt.Sprintf("/ivr/c/%s/handle?action=start&connection=1", testdata.TwilioChannel.UUID),
			form:               nil,
			expectedStatus:     200,
			expectedContains:   []string{`<Gather numDigits="1" timeout="30"`, `<Say>Hello there. Please enter one or two.  This flow was triggered by Cathy</Say>`},
			expectedConnStatus: map[string]string{"Call1": "I", "Call2": "W", "Call3": "W"},
		},
		{
			label: "handle resume but without digits we're waiting for",
			url:   fmt.Sprintf("/ivr/c/%s/handle?action=resume&connection=1", testdata.TwilioChannel.UUID),
			form: url.Values{
				"CallStatus": []string{"in-progress"},
				"wait_type":  []string{"gather"},
				"timeout":    []string{"true"},
			},
			expectedStatus:     200,
			expectedContains:   []string{`<Gather numDigits="1" timeout="30"`, `<Say>Sorry, that is not one or two, try again.</Say>`},
			expectedConnStatus: map[string]string{"Call1": "I", "Call2": "W", "Call3": "W"},
		},
		{
			label: "handle resume with digits we're waiting for",
			url:   fmt.Sprintf("/ivr/c/%s/handle?action=resume&connection=1", testdata.TwilioChannel.UUID),
			form: url.Values{
				"CallStatus": []string{"in-progress"},
				"wait_type":  []string{"gather"},
				"Digits":     []string{"1"},
			},
			expectedStatus:     200,
			expectedContains:   []string{`<Gather timeout="30"`, `<Say>Great! You said One. Ok, now enter a number 1 to 100 then press pound.</Say>`},
			expectedConnStatus: map[string]string{"Call1": "I", "Call2": "W", "Call3": "W"},
		},
		{
			label: "handle resume with digits that are out of range specified in flow",
			url:   fmt.Sprintf("/ivr/c/%s/handle?action=resume&connection=1", testdata.TwilioChannel.UUID),
			form: url.Values{
				"CallStatus": []string{"in-progress"},
				"wait_type":  []string{"gather"},
				"Digits":     []string{"101"},
			},
			expectedStatus:     200,
			expectedContains:   []string{`<Gather timeout="30"`, `<Say>Sorry, that&#39;s too big. Enter a number 1 to 100 then press pound.</Say>`},
			expectedConnStatus: map[string]string{"Call1": "I", "Call2": "W", "Call3": "W"},
		},
		{
			label: "handle resume with digits that are in range specified in flow",
			url:   fmt.Sprintf("/ivr/c/%s/handle?action=resume&connection=1", testdata.TwilioChannel.UUID),
			form: url.Values{
				"CallStatus": []string{"in-progress"},
				"wait_type":  []string{"gather"},
				"Digits":     []string{"56"},
			},
			expectedStatus:     200,
			expectedContains:   []string{`<Say>You picked the number 56, excellent choice. Ok now tell me briefly why you are happy today.</Say>`, `<Record action=`},
			expectedConnStatus: map[string]string{"Call1": "I", "Call2": "W", "Call3": "W"},
		},
		{
			label: "handle resume with missing recording that should start a call forward",
			url:   fmt.Sprintf("/ivr/c/%s/handle?action=resume&connection=1", testdata.TwilioChannel.UUID),
			form: url.Values{
				"CallStatus": []string{"in-progress"},
				"wait_type":  []string{"record"},
				// no recording as we don't have S3 to back us up, flow just moves forward
			},
			expectedStatus: 200,
			expectedContains: []string{
				`<Say>You said</Say>`,
				`<Say>I hope hearing that makes you feel better. Good day and good bye.</Say>`,
				`<Dial action=`,
				`>2065551212</Dial>`,
			},
			expectedConnStatus: map[string]string{"Call1": "I", "Call2": "W", "Call3": "W"},
		},
		{
			label: "handle resume call forwarding result",
			url:   fmt.Sprintf("/ivr/c/%s/handle?action=resume&connection=1", testdata.TwilioChannel.UUID),
			form: url.Values{
				"CallStatus":     []string{"in-progress"},
				"DialCallStatus": []string{"answered"},
				"wait_type":      []string{"dial"},
			},
			expectedStatus:     200,
			expectedContains:   []string{`<Say>Great, they answered.</Say>`, `<Hangup></Hangup>`},
			expectedConnStatus: map[string]string{"Call1": "I", "Call2": "W", "Call3": "W"},
		},
		{
			label: "status update that call 1 is complete",
			url:   fmt.Sprintf("/ivr/c/%s/status", testdata.TwilioChannel.UUID),
			form: url.Values{
				"CallSid":      []string{"Call1"},
				"CallStatus":   []string{"completed"},
				"CallDuration": []string{"50"},
			},
			expectedStatus:     200,
			expectedResponse:   `<Response><!--status updated: D--></Response>`,
			expectedConnStatus: map[string]string{"Call1": "D", "Call2": "W", "Call3": "W"},
		},
		{
			label:              "start call 2",
			url:                fmt.Sprintf("/ivr/c/%s/handle?action=start&connection=2", testdata.TwilioChannel.UUID),
			form:               nil,
			expectedStatus:     200,
			expectedContains:   []string{"Hello there. Please enter one or two."},
			expectedConnStatus: map[string]string{"Call1": "D", "Call2": "I", "Call3": "W"},
		},
		{
			label: "resume with status that says call completed on Twilio side",
			url:   fmt.Sprintf("/ivr/c/%s/handle?action=resume&connection=2", testdata.TwilioChannel.UUID),
			form: url.Values{
				"CallStatus": []string{"completed"},
				"wait_type":  []string{"gather"},
				"Digits":     []string{"56"},
			},
			expectedStatus:     200,
			expectedResponse:   `<Response><!--call completed--><Say>An error has occurred, please try again later.</Say><Hangup></Hangup></Response>`,
			expectedConnStatus: map[string]string{"Call1": "D", "Call2": "D", "Call3": "W"},
		},
		{
			label: "call3 started with answered_by telling us it's a machine",
			url:   fmt.Sprintf("/ivr/c/%s/handle?action=start&connection=3", testdata.TwilioChannel.UUID),
			form: url.Values{
				"CallStatus": []string{"in-progress"},
				"AnsweredBy": []string{"machine_start"},
			},
			expectedStatus:     200,
			expectedContains:   []string{`<Response><!--status updated: E, next_attempt: `, `<Say>An error has occurred, please try again later.</Say><Hangup></Hangup></Response>`},
			expectedConnStatus: map[string]string{"Call1": "D", "Call2": "D", "Call3": "E"},
		},
		{
			label: "then Twilio will call the status callback to say that we're done but don't overwrite the error status",
			url:   fmt.Sprintf("/ivr/c/%s/status", testdata.TwilioChannel.UUID),
			form: url.Values{
				"CallSid":      []string{"Call3"},
				"CallStatus":   []string{"completed"},
				"CallDuration": []string{"50"},
			},
			expectedStatus:     200,
			expectedResponse:   `<Response><!--status D ignored, already errored--></Response>`,
			expectedConnStatus: map[string]string{"Call1": "D", "Call2": "D", "Call3": "E"},
		},
		{
			label: "we don't have a call trigger so incoming call creates a missed call event",
			url:   fmt.Sprintf("/ivr/c/%s/incoming", testdata.TwilioChannel.UUID),
			form: url.Values{
				"CallSid":    []string{"Call4"},
				"CallStatus": []string{"ringing"},
				"Caller":     []string{"+12065551212"},
			},
			expectedStatus:     200,
			expectedResponse:   `<Response><!--missed call handled--></Response>`,
			expectedConnStatus: map[string]string{"Call1": "D", "Call2": "D", "Call3": "E", "Call4": "I"},
		},
		{
			label: "",
			url:   fmt.Sprintf("/ivr/c/%s/status", testdata.TwilioChannel.UUID),
			form: url.Values{
				"CallSid":      []string{"Call4"},
				"CallStatus":   []string{"failed"},
				"CallDuration": []string{"50"},
			},
			expectedStatus:     200,
			expectedResponse:   `<Response><!--no flow start found, status updated: F--></Response>`,
			expectedConnStatus: map[string]string{"Call1": "D", "Call2": "D", "Call3": "E", "Call4": "F"},
		},
	}

	for i, tc := range tcs {
		mrUrl := "http://localhost:8090/mr" + tc.url

		req, err := http.NewRequest(http.MethodPost, mrUrl, strings.NewReader(tc.form.Encode()))
		assert.NoError(t, err)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		assert.Equal(t, tc.expectedStatus, resp.StatusCode, "status code mismatch in %s", tc.label)

		body, _ := io.ReadAll(resp.Body)

		if tc.expectedResponse != "" {
			assert.Equal(t, `<?xml version="1.0" encoding="UTF-8"?>`+"\n"+tc.expectedResponse, string(body), "response mismatch in %s", tc.label)
		}

		for _, needle := range tc.expectedContains {
			assert.Containsf(t, string(body), needle, "%d does not contain expected body", i)
		}

		for connExtID, expStatus := range tc.expectedConnStatus {
			assertdb.Query(t, db, `SELECT status FROM channels_channelconnection WHERE external_id = $1`, connExtID).
				Columns(map[string]interface{}{"status": expStatus}, "status mismatch for connection '%s' in test '%s'", connExtID, tc.label)
		}
	}

	// check our final state of sessions, runs, msgs, connections
	assertdb.Query(t, db, `SELECT count(*) FROM flows_flowsession WHERE contact_id = $1 AND status = 'C'`, testdata.Cathy.ID).Returns(1)

	assertdb.Query(t, db, `SELECT count(*) FROM flows_flowrun WHERE contact_id = $1 AND status = 'C'`, testdata.Cathy.ID).Returns(1)

	assertdb.Query(t, db, `SELECT count(*) FROM msgs_msg WHERE contact_id = $1 AND msg_type = 'V' AND status = 'W' AND direction = 'O'`, testdata.Cathy.ID).Returns(8)

	assertdb.Query(t, db, `SELECT count(*) FROM msgs_msg WHERE contact_id = $1 AND msg_type = 'V' AND status = 'H' AND direction = 'I'`, testdata.Cathy.ID).Returns(5)

	assertdb.Query(t, db, `SELECT count(*) FROM channels_channellog WHERE connection_id = 1 AND channel_id IS NOT NULL`).Returns(9)

	assertdb.Query(t, db, `SELECT count(*) FROM msgs_msg WHERE contact_id = $1 AND msg_type = 'V' 
		AND ((status = 'H' AND direction = 'I') OR (status = 'W' AND direction = 'O'))`, testdata.Bob.ID).Returns(2)
}

func mockVonageHandler(w http.ResponseWriter, r *http.Request) {
	if r.URL.Query().Get("recording") != "" {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte{})
	} else {
		type CallForm struct {
			To []struct {
				Number string `json:"number"`
			} `json:"to"`
			Action string `json:"action,omitempty"`
		}
		body, _ := io.ReadAll(r.Body)
		form := &CallForm{}
		json.Unmarshal(body, form)
		logrus.WithField("method", r.Method).WithField("url", r.URL.String()).WithField("body", string(body)).WithField("form", form).Info("test server called")

		// end of a leg
		if form.Action == "transfer" {
			w.WriteHeader(http.StatusNoContent)
		} else if form.To[0].Number == "16055741111" {
			w.WriteHeader(http.StatusCreated)
			w.Write([]byte(`{ "uuid": "Call1","status": "started","direction": "outbound","conversation_uuid": "Conversation1"}`))
		} else if form.To[0].Number == "16055743333" {
			w.WriteHeader(http.StatusCreated)
			w.Write([]byte(`{ "uuid": "Call2","status": "started","direction": "outbound","conversation_uuid": "Conversation2"}`))
		} else if form.To[0].Number == "2065551212" {
			// start of a transfer leg
			w.WriteHeader(http.StatusCreated)
			w.Write([]byte(`{ "uuid": "Call3","status": "started","direction": "outbound","conversation_uuid": "Conversation3"}`))
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}
}

func TestVonageIVR(t *testing.T) {
	ctx, rt, db, rp := testsuite.Get()
	rc := rp.Get()
	defer rc.Close()

	defer testsuite.Reset(testsuite.ResetAll)

	// deactivate our twilio channel
	db.MustExec(`UPDATE channels_channel SET is_active = FALSE WHERE id = $1`, testdata.TwilioChannel.ID)

	// add auth tokens
	db.MustExec(`UPDATE channels_channel SET config = '{"nexmo_app_id": "app_id", "nexmo_app_private_key": "-----BEGIN PRIVATE KEY-----\nMIICdgIBADANBgkqhkiG9w0BAQEFAASCAmAwggJcAgEAAoGBAKNwapOQ6rQJHetP\nHRlJBIh1OsOsUBiXb3rXXE3xpWAxAha0MH+UPRblOko+5T2JqIb+xKf9Vi3oTM3t\nKvffaOPtzKXZauscjq6NGzA3LgeiMy6q19pvkUUOlGYK6+Xfl+B7Xw6+hBMkQuGE\nnUS8nkpR5mK4ne7djIyfHFfMu4ptAgMBAAECgYA+s0PPtMq1osG9oi4xoxeAGikf\nJB3eMUptP+2DYW7mRibc+ueYKhB9lhcUoKhlQUhL8bUUFVZYakP8xD21thmQqnC4\nf63asad0ycteJMLb3r+z26LHuCyOdPg1pyLk3oQ32lVQHBCYathRMcVznxOG16VK\nI8BFfstJTaJu0lK/wQJBANYFGusBiZsJQ3utrQMVPpKmloO2++4q1v6ZR4puDQHx\nTjLjAIgrkYfwTJBLBRZxec0E7TmuVQ9uJ+wMu/+7zaUCQQDDf2xMnQqYknJoKGq+\noAnyC66UqWC5xAnQS32mlnJ632JXA0pf9pb1SXAYExB1p9Dfqd3VAwQDwBsDDgP6\nHD8pAkEA0lscNQZC2TaGtKZk2hXkdcH1SKru/g3vWTkRHxfCAznJUaza1fx0wzdG\nGcES1Bdez0tbW4llI5By/skZc2eE3QJAFl6fOskBbGHde3Oce0F+wdZ6XIJhEgCP\niukIcKZoZQzoiMJUoVRrA5gqnmaYDI5uRRl/y57zt6YksR3KcLUIuQJAd242M/WF\n6YAZat3q/wEeETeQq1wrooew+8lHl05/Nt0cCpV48RGEhJ83pzBm3mnwHf8lTBJH\nx6XroMXsmbnsEw==\n-----END PRIVATE KEY-----", "callback_domain": "localhost:8090"}', role='SRCA' WHERE id = $1`, testdata.VonageChannel.ID)

	// start test server
	ts := httptest.NewServer(http.HandlerFunc(mockVonageHandler))
	defer ts.Close()

	wg := &sync.WaitGroup{}
	server := web.NewServer(ctx, rt, wg)
	server.Start()
	defer server.Stop()

	vonage.CallURL = ts.URL
	vonage.IgnoreSignatures = true

	// create a flow start for cathy and george
	extra := json.RawMessage(`{"ref_id":"123"}`)
	start := models.NewFlowStart(testdata.Org1.ID, models.StartTypeTrigger, models.FlowTypeVoice, testdata.IVRFlow.ID, models.DoRestartParticipants, models.DoIncludeActive).
		WithContactIDs([]models.ContactID{testdata.Cathy.ID, testdata.George.ID}).
		WithExtra(extra)
	models.InsertFlowStarts(ctx, db, []*models.FlowStart{start})

	// call our master starter
	err := starts.CreateFlowBatches(ctx, rt, start)
	assert.NoError(t, err)

	// start our task
	task, err := queue.PopNextTask(rc, queue.HandlerQueue)
	assert.NoError(t, err)
	batch := &models.FlowStartBatch{}
	err = json.Unmarshal(task.Task, batch)
	assert.NoError(t, err)

	// request our call to start
	err = ivr_tasks.HandleFlowStartBatch(ctx, rt, batch)
	assert.NoError(t, err)

	assertdb.Query(t, db, `SELECT COUNT(*) FROM channels_channelconnection WHERE contact_id = $1 AND status = $2 AND external_id = $3`,
		testdata.Cathy.ID, models.ConnectionStatusWired, "Call1").Returns(1)

	assertdb.Query(t, db, `SELECT COUNT(*) FROM channels_channelconnection WHERE contact_id = $1 AND status = $2 AND external_id = $3`,
		testdata.George.ID, models.ConnectionStatusWired, "Call2").Returns(1)

	tcs := []struct {
		label            string
		url              string
		body             string
		expectedStatus   int
		expectedResponse string
		contains         []string
	}{
		{
			label:          "handle start on wired connection",
			url:            fmt.Sprintf("/ivr/c/%s/handle?action=start&connection=1", testdata.VonageChannel.UUID),
			body:           `{"from":"12482780345","to":"12067799294","uuid":"80c9a606-717e-48b9-ae22-ce00269cbb08","conversation_uuid":"CON-f90649c3-cbf3-42d6-9ab1-01503befac1c"}`,
			expectedStatus: 200,
			expectedResponse: `[
				{
					"action": "talk",
					"bargeIn": true,
					"text": "Hello there. Please enter one or two. Your reference id is 123"
				},
				{
					"action": "input",
					"eventMethod": "POST",
					"eventUrl": [
						"https://localhost:8090/mr/ivr/c/19012bfd-3ce3-4cae-9bb9-76cf92c73d49/handle?action=resume&connection=1&urn=tel%3A%2B16055741111%3Fid%3D10000%26priority%3D1000&wait_type=gather&sig=KqiNYjpUMvqtJLX3Pi9tmL03XVk%3D"
					],
					"maxDigits": 1,
					"submitOnHash": true,
					"timeOut": 30
				}
			]`,
		},
		{
			label:          "handle resume with invalid digit",
			url:            fmt.Sprintf("/ivr/c/%s/handle?action=resume&connection=1&wait_type=gather", testdata.VonageChannel.UUID),
			body:           `{"dtmf":"3","timed_out":false,"uuid":null,"conversation_uuid":"CON-f90649c3-cbf3-42d6-9ab1-01503befac1c","timestamp":"2019-04-01T21:08:54.680Z"}`,
			expectedStatus: 200,
			contains:       []string{"Sorry, that is not one or two, try again."},
		},
		{
			label:          "handle resume with valid digit",
			url:            fmt.Sprintf("/ivr/c/%s/handle?action=resume&connection=1&wait_type=gather", testdata.VonageChannel.UUID),
			body:           `{"dtmf":"1","timed_out":false,"uuid":null,"conversation_uuid":"CON-f90649c3-cbf3-42d6-9ab1-01503befac1c","timestamp":"2019-04-01T21:08:54.680Z"}`,
			expectedStatus: 200,
			contains:       []string{"Great! You said One."},
		},
		{
			label:          "handle resume with digits out of range in flow",
			url:            fmt.Sprintf("/ivr/c/%s/handle?action=resume&connection=1&wait_type=gather", testdata.VonageChannel.UUID),
			body:           `{"dtmf":"101","timed_out":false,"uuid":null,"conversation_uuid":"CON-f90649c3-cbf3-42d6-9ab1-01503befac1c","timestamp":"2019-04-01T21:08:54.680Z"}`,
			expectedStatus: 200,
			contains:       []string{"too big"},
		},
		{
			label:          "handle resume with digits within range in flow",
			url:            fmt.Sprintf("/ivr/c/%s/handle?action=resume&connection=1&wait_type=gather", testdata.VonageChannel.UUID),
			body:           `{"dtmf":"56","timed_out":false,"uuid":null,"conversation_uuid":"CON-f90649c3-cbf3-42d6-9ab1-01503befac1c","timestamp":"2019-04-01T21:08:54.680Z"}`,
			expectedStatus: 200,
			contains:       []string{"You picked the number 56"},
		},
		{
			label:          "recording callback",
			url:            fmt.Sprintf("/ivr/c/%s/handle?action=resume&connection=1&wait_type=recording_url&recording_uuid=0c15f253-8e67-45c8-9980-7d38292edd3c", testdata.VonageChannel.UUID),
			body:           fmt.Sprintf(`{"recording_url": "%s", "end_time":"2019-04-01T21:08:56.000Z","uuid":"Call1","network":"310260","status":"answered","direction":"outbound","timestamp":"2019-04-01T21:08:56.342Z"}`, ts.URL+"?recording=true"),
			expectedStatus: 200,
			contains:       []string{"inserted recording url"},
		},
		{
			label:          "resume with recording",
			url:            fmt.Sprintf("/ivr/c/%s/handle?action=resume&connection=1&wait_type=record&recording_uuid=0c15f253-8e67-45c8-9980-7d38292edd3c", testdata.VonageChannel.UUID),
			body:           `{"end_time":"2019-04-01T21:08:56.000Z","uuid":"Call1","network":"310260","status":"answered","direction":"outbound","timestamp":"2019-04-01T21:08:56.342Z", "recording_url": "http://foo.bar/"}`,
			expectedStatus: 200,
			contains:       []string{"I hope hearing that makes you feel better.", `"action": "conversation"`},
		},
		{
			label:            "transfer answered",
			url:              fmt.Sprintf("/ivr/c/%s/status", testdata.VonageChannel.UUID),
			body:             `{"uuid": "Call3", "status": "answered"}`,
			expectedStatus:   200,
			expectedResponse: `{"_message":"updated status for call: Call1 to: answered"}`,
		},
		{
			label:            "transfer completed",
			url:              fmt.Sprintf("/ivr/c/%s/status", testdata.VonageChannel.UUID),
			body:             `{"uuid": "Call3", "duration": "25", "status": "completed"}`,
			expectedStatus:   200,
			expectedResponse: `{"_message":"reconnected call: Call1 to flow with dial status: answered"}`,
		},
		{
			label:          "transfer callback",
			url:            fmt.Sprintf("/ivr/c/%s/handle?action=resume&connection=1&wait_type=dial&dial_status=answered&dial_duration=25", testdata.VonageChannel.UUID),
			expectedStatus: 200,
			contains:       []string{"Great, they answered."},
		},
		{
			label:            "call complete",
			url:              fmt.Sprintf("/ivr/c/%s/status", testdata.VonageChannel.UUID),
			body:             `{"end_time":"2019-04-01T21:08:56.000Z","uuid":"Call1","network":"310260","duration":"50","start_time":"2019-04-01T21:08:42.000Z","rate":"0.01270000","price":"0.00296333","from":"12482780345","to":"12067799294","conversation_uuid":"CON-f90649c3-cbf3-42d6-9ab1-01503befac1c","status":"completed","direction":"outbound","timestamp":"2019-04-01T21:08:56.342Z"}`,
			expectedStatus:   200,
			expectedResponse: `{"_message":"status updated: D"}`,
		},
		{
			label:          "new call",
			url:            fmt.Sprintf("/ivr/c/%s/handle?action=start&connection=2", testdata.VonageChannel.UUID),
			body:           `{"from":"12482780345","to":"12067799294","uuid":"Call2","conversation_uuid":"CON-f90649c3-cbf3-42d6-9ab1-01503befac1c"}`,
			expectedStatus: 200,
			expectedResponse: `[
				{
					"action": "talk",
					"bargeIn": true,
					"text": "Hello there. Please enter one or two. Your reference id is 123"
				},
				{
					"action": "input",
					"eventMethod": "POST",
					"eventUrl": [
						"https://localhost:8090/mr/ivr/c/19012bfd-3ce3-4cae-9bb9-76cf92c73d49/handle?action=resume&connection=2&urn=tel%3A%2B16055743333%3Fid%3D10002%26priority%3D1000&wait_type=gather&sig=QbU8c2ChHdJln%2BE5wUi%2BR6mF0nY%3D"
					],
					"maxDigits": 1,
					"submitOnHash": true,
					"timeOut": 30
				}
			]`,
		},
		{
			label:          "new call dtmf 1",
			url:            fmt.Sprintf("/ivr/c/%s/handle?action=resume&connection=2&wait_type=gather", testdata.VonageChannel.UUID),
			body:           `{"dtmf":"1","timed_out":false,"uuid":"Call2","conversation_uuid":"CON-f90649c3-cbf3-42d6-9ab1-01503befac1c","timestamp":"2019-04-01T21:08:54.680Z"}`,
			expectedStatus: 200,
			expectedResponse: `[
				{
					"action": "talk",
					"bargeIn": true,
					"text": "Great! You said One. Ok, now enter a number 1 to 100 then press pound."
				},
				{
					"action": "input",
					"eventMethod": "POST",
					"eventUrl": [
						"https://localhost:8090/mr/ivr/c/19012bfd-3ce3-4cae-9bb9-76cf92c73d49/handle?action=resume&connection=2&urn=tel%3A%2B16055743333%3Fid%3D10002%26priority%3D1000&wait_type=gather&sig=QbU8c2ChHdJln%2BE5wUi%2BR6mF0nY%3D"
					],
					"maxDigits": 20,
					"submitOnHash": true,
					"timeOut": 30
				}
			]`,
		},
		{
			label:            "new call ended",
			url:              fmt.Sprintf("/ivr/c/%s/status", testdata.VonageChannel.UUID),
			body:             `{"end_time":"2019-04-01T21:08:56.000Z","uuid":"Call2","network":"310260","duration":"50","start_time":"2019-04-01T21:08:42.000Z","rate":"0.01270000","price":"0.00296333","from":"12482780345","to":"12067799294","conversation_uuid":"CON-f90649c3-cbf3-42d6-9ab1-01503befac1c","status":"completed","direction":"outbound","timestamp":"2019-04-01T21:08:56.342Z"}`,
			expectedStatus:   200,
			expectedResponse: `{"_message":"status updated: D"}`,
		},
		{
			label:            "incoming call",
			url:              fmt.Sprintf("/ivr/c/%s/incoming", testdata.VonageChannel.UUID),
			body:             `{"from":"12482780345","to":"12067799294","uuid":"Call4","conversation_uuid":"CON-f90649c3-cbf3-42d6-9ab1-01503befac1c"}`,
			expectedStatus:   200,
			expectedResponse: `{"_message":"missed call handled"}`,
		},
		{
			label:            "failed call",
			url:              fmt.Sprintf("/ivr/c/%s/status", testdata.VonageChannel.UUID),
			body:             `{"end_time":"2019-04-01T21:08:56.000Z","uuid":"Call4","network":"310260","duration":"50","start_time":"2019-04-01T21:08:42.000Z","rate":"0.01270000","price":"0.00296333","from":"12482780345","to":"12067799294","conversation_uuid":"CON-f90649c3-cbf3-42d6-9ab1-01503befac1c","status":"failed","direction":"outbound","timestamp":"2019-04-01T21:08:56.342Z"}`,
			expectedStatus:   200,
			expectedResponse: `{"_message":"no flow start found, status updated: F"}`,
		},
	}

	for _, tc := range tcs {
		mrUrl := "http://localhost:8090/mr" + tc.url

		req, err := http.NewRequest(http.MethodPost, mrUrl, strings.NewReader(tc.body))
		assert.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		assert.Equal(t, tc.expectedStatus, resp.StatusCode, "status code mismatch in %s", tc.label)

		body, _ := io.ReadAll(resp.Body)

		if tc.expectedResponse != "" {
			test.AssertEqualJSON(t, []byte(tc.expectedResponse), body, "response mismatch in %s", tc.label)
		}

		for _, needle := range tc.contains {
			if !assert.Containsf(t, string(body), needle, "testcase '%s' does not contain expected body", tc.label) {
				t.FailNow()
			}
		}
	}

	// check our final state of sessions, runs, msgs, connections
	assertdb.Query(t, db, `SELECT count(*) FROM flows_flowsession WHERE contact_id = $1 AND status = 'C'`, testdata.Cathy.ID).Returns(1)

	assertdb.Query(t, db, `SELECT count(*) FROM flows_flowrun WHERE contact_id = $1 AND is_active = FALSE`, testdata.Cathy.ID).Returns(1)

	assertdb.Query(t, db, `SELECT count(*) FROM channels_channelconnection WHERE contact_id = $1 AND status = 'D' AND duration = 50`, testdata.Cathy.ID).Returns(1)

	assertdb.Query(t, db, `SELECT count(*) FROM msgs_msg WHERE contact_id = $1 AND msg_type = 'V' AND status = 'W' AND direction = 'O'`, testdata.Cathy.ID).Returns(9)

	assertdb.Query(t, db, `SELECT count(*) FROM channels_channelconnection WHERE status = 'F' AND direction = 'I'`).Returns(1)

	assertdb.Query(t, db, `SELECT count(*) FROM msgs_msg WHERE contact_id = $1 AND msg_type = 'V' AND status = 'H' AND direction = 'I'`, testdata.Cathy.ID).Returns(5)

	assertdb.Query(t, db, `SELECT count(*) FROM channels_channellog WHERE connection_id = 1 AND channel_id IS NOT NULL`).Returns(10)

	assertdb.Query(t, db, `SELECT count(*) FROM msgs_msg WHERE contact_id = $1 AND msg_type = 'V' AND ((status = 'H' AND direction = 'I') OR (status = 'W' AND direction = 'O'))`, testdata.George.ID).Returns(3)

	assertdb.Query(t, db, `SELECT count(*) FROM channels_channelconnection WHERE status = 'D' AND contact_id = $1`, testdata.George.ID).Returns(1)
}
