package ivr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/nyaruka/goflow/test"
	"github.com/nyaruka/mailroom/config"
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
	"github.com/nyaruka/mailroom/core/ivr/twiml"
	"github.com/nyaruka/mailroom/core/ivr/vonage"
	ivr_tasks "github.com/nyaruka/mailroom/core/tasks/ivr"
)

func TestTwilioIVR(t *testing.T) {
	ctx, _, db, rp := testsuite.Get()
	rc := rp.Get()
	defer rc.Close()

	defer func() {
		testsuite.ResetStorage()
		testsuite.Reset()
	}()

	// start test server
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		logrus.WithField("method", r.Method).WithField("url", r.URL.String()).WithField("form", r.Form).Info("test server called")
		if strings.HasSuffix(r.URL.String(), "Calls.json") {
			to := r.Form.Get("To")
			if to == "+16055741111" {
				w.WriteHeader(http.StatusCreated)
				w.Write([]byte(`{"sid": "Call1"}`))
			} else if to == "+16055743333" {
				w.WriteHeader(http.StatusCreated)
				w.Write([]byte(`{"sid": "Call2"}`))
			} else {
				w.WriteHeader(http.StatusNotFound)
			}
		}
		if strings.HasSuffix(r.URL.String(), "recording.mp3") {
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	twiml.BaseURL = ts.URL
	twiml.IgnoreSignatures = true

	wg := &sync.WaitGroup{}
	server := web.NewServer(ctx, config.Mailroom, db, rp, testsuite.MediaStorage(), nil, wg)
	server.Start()
	defer server.Stop()

	// add auth tokens
	db.MustExec(`UPDATE channels_channel SET config = '{"auth_token": "token", "account_sid": "sid", "callback_domain": "localhost:8090"}' WHERE id = $1`, testdata.TwilioChannel.ID)

	// create a flow start for cathy and george
	parentSummary := json.RawMessage(`{"flow": {"name": "IVR Flow", "uuid": "2f81d0ea-4d75-4843-9371-3f7465311cce"}, "uuid": "8bc73097-ac57-47fb-82e5-184f8ec6dbef", "status": "active", "contact": {"id": 10000, "name": "Cathy", "urns": ["tel:+16055741111?id=10000&priority=50"], "uuid": "6393abc0-283d-4c9b-a1b3-641a035c34bf", "fields": {"gender": {"text": "F"}}, "groups": [{"name": "Doctors", "uuid": "c153e265-f7c9-4539-9dbc-9b358714b638"}], "timezone": "America/Los_Angeles", "created_on": "2019-07-23T09:35:01.439614-07:00"}, "results": {}}`)

	start := models.NewFlowStart(testdata.Org1.ID, models.StartTypeTrigger, models.FlowTypeVoice, testdata.IVRFlow.ID, models.DoRestartParticipants, models.DoIncludeActive).
		WithContactIDs([]models.ContactID{testdata.Cathy.ID, testdata.George.ID}).
		WithParentSummary(parentSummary)

	err := models.InsertFlowStarts(ctx, db, []*models.FlowStart{start})
	assert.NoError(t, err)

	// call our master starter
	err = starts.CreateFlowBatches(ctx, db, rp, nil, start)
	assert.NoError(t, err)

	// start our task
	task, err := queue.PopNextTask(rc, queue.HandlerQueue)
	assert.NoError(t, err)
	batch := &models.FlowStartBatch{}
	err = json.Unmarshal(task.Task, batch)
	assert.NoError(t, err)

	// request our call to start
	err = ivr_tasks.HandleFlowStartBatch(ctx, config.Mailroom, db, rp, batch)
	assert.NoError(t, err)

	testsuite.AssertQuery(t, db, `SELECT COUNT(*) FROM channels_channelconnection WHERE contact_id = $1 AND status = $2 AND external_id = $3`,
		testdata.Cathy.ID, models.ConnectionStatusWired, "Call1").Returns(1)

	testsuite.AssertQuery(t, db,
		`SELECT COUNT(*) FROM channels_channelconnection WHERE contact_id = $1 AND status = $2 AND external_id = $3`,
		testdata.George.ID, models.ConnectionStatusWired, "Call2").Returns(1)

	tcs := []struct {
		action           string
		channel          *testdata.Channel
		connectionID     models.ConnectionID
		form             url.Values
		expectedStatus   int
		expectedResponse string
		contains         []string
	}{
		{
			action:         "start",
			channel:        testdata.TwilioChannel,
			connectionID:   models.ConnectionID(1),
			form:           nil,
			expectedStatus: 200,
			contains:       []string{"Hello there. Please enter one or two.  This flow was triggered by Cathy"},
		},
		{
			action:       "resume",
			channel:      testdata.TwilioChannel,
			connectionID: models.ConnectionID(1),
			form: url.Values{
				"CallStatus": []string{"in-progress"},
				"wait_type":  []string{"gather"},
				"timeout":    []string{"true"},
			},
			expectedStatus: 200,
			contains:       []string{"Sorry, that is not one or two, try again."},
		},
		{
			action:       "resume",
			channel:      testdata.TwilioChannel,
			connectionID: models.ConnectionID(1),
			form: url.Values{
				"CallStatus": []string{"in-progress"},
				"wait_type":  []string{"gather"},
				"Digits":     []string{"1"},
			},
			expectedStatus: 200,
			contains:       []string{"Great! You said One."},
		},
		{
			action:       "resume",
			channel:      testdata.TwilioChannel,
			connectionID: models.ConnectionID(1),
			form: url.Values{
				"CallStatus": []string{"in-progress"},
				"wait_type":  []string{"gather"},
				"Digits":     []string{"101"},
			},
			expectedStatus: 200,
			contains:       []string{"too big"},
		},
		{
			action:       "resume",
			channel:      testdata.TwilioChannel,
			connectionID: models.ConnectionID(1),
			form: url.Values{
				"CallStatus": []string{"in-progress"},
				"wait_type":  []string{"gather"},
				"Digits":     []string{"56"},
			},
			expectedStatus: 200,
			contains:       []string{"You picked the number 56"},
		},
		{
			action:       "resume",
			channel:      testdata.TwilioChannel,
			connectionID: models.ConnectionID(1),
			form: url.Values{
				"CallStatus": []string{"in-progress"},
				"wait_type":  []string{"record"},
				// no recording as we don't have S3 to back us up, flow just moves forward
			},
			expectedStatus: 200,
			contains: []string{
				"I hope hearing that makes you feel better",
				"<Dial ",
				"2065551212",
			},
		},
		{
			action:       "resume",
			channel:      testdata.TwilioChannel,
			connectionID: models.ConnectionID(1),
			form: url.Values{
				"CallStatus":     []string{"in-progress"},
				"DialCallStatus": []string{"answered"},
				"wait_type":      []string{"dial"},
			},
			expectedStatus: 200,
			contains: []string{
				"Great, they answered.",
				"<Hangup",
			},
		},
		{
			action:       "status",
			channel:      testdata.TwilioChannel,
			connectionID: models.ConnectionID(1),
			form: url.Values{
				"CallSid":      []string{"Call1"},
				"CallStatus":   []string{"completed"},
				"CallDuration": []string{"50"},
			},
			expectedStatus: 200,
			contains:       []string{"status updated: D"},
		},
		{
			action:         "start",
			channel:        testdata.TwilioChannel,
			connectionID:   models.ConnectionID(2),
			form:           nil,
			expectedStatus: 200,
			contains:       []string{"Hello there. Please enter one or two."},
		},
		{
			action:       "resume",
			channel:      testdata.TwilioChannel,
			connectionID: models.ConnectionID(2),
			form: url.Values{
				"CallStatus": []string{"completed"},
				"wait_type":  []string{"gather"},
				"Digits":     []string{"56"},
			},
			expectedStatus: 200,
			contains:       []string{"<!--call completed-->"},
		},
		{
			action:       "incoming",
			channel:      testdata.TwilioChannel,
			connectionID: models.ConnectionID(3),
			form: url.Values{
				"CallSid":    []string{"Call2"},
				"CallStatus": []string{"completed"},
				"Caller":     []string{"+12065551212"},
			},
			expectedStatus: 200,
			contains:       []string{"missed call handled"},
		},
		{
			action:       "status",
			channel:      testdata.TwilioChannel,
			connectionID: models.ConnectionID(3),
			form: url.Values{
				"CallSid":      []string{"Call2"},
				"CallStatus":   []string{"failed"},
				"CallDuration": []string{"50"},
			},
			expectedStatus:   200,
			expectedResponse: "<Response><!--no flow start found, status updated: F--></Response>",
		},
	}

	for i, tc := range tcs {
		form := url.Values{
			"action":     []string{tc.action},
			"connection": []string{fmt.Sprintf("%d", tc.connectionID)},
		}
		url := fmt.Sprintf("http://localhost:8090/mr/ivr/c/%s/handle", tc.channel.UUID) + "?" + form.Encode()
		if tc.action == "status" {
			url = fmt.Sprintf("http://localhost:8090/mr/ivr/c/%s/status", tc.channel.UUID)
		}
		if tc.action == "incoming" {
			url = fmt.Sprintf("http://localhost:8090/mr/ivr/c/%s/incoming", tc.channel.UUID)
		}
		req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(tc.form.Encode()))
		assert.NoError(t, err)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		assert.Equal(t, tc.expectedStatus, resp.StatusCode, "%d: status code mismatch", i)

		body, _ := ioutil.ReadAll(resp.Body)

		if tc.expectedResponse != "" {
			assert.Equal(t, `<?xml version="1.0" encoding="UTF-8"?>`+"\n"+tc.expectedResponse, string(body), "%d: response mismatch", i)
		}

		for _, needle := range tc.contains {
			assert.Containsf(t, string(body), needle, "%d does not contain expected body", i)
		}
	}

	// check our final state of sessions, runs, msgs, connections
	testsuite.AssertQuery(t, db, `SELECT count(*) FROM flows_flowsession WHERE contact_id = $1 AND status = 'C'`, testdata.Cathy.ID).Returns(1)

	testsuite.AssertQuery(t, db, `SELECT count(*) FROM flows_flowrun WHERE contact_id = $1 AND is_active = FALSE`, testdata.Cathy.ID).Returns(1)

	testsuite.AssertQuery(t, db, `SELECT count(*) FROM channels_channelconnection WHERE contact_id = $1 AND status = 'D' AND duration = 50`, testdata.Cathy.ID).Returns(1)

	testsuite.AssertQuery(t, db, `SELECT count(*) FROM msgs_msg WHERE contact_id = $1 AND msg_type = 'V' AND connection_id = 1 AND status = 'W' AND direction = 'O'`, testdata.Cathy.ID).Returns(8)

	testsuite.AssertQuery(t, db, `SELECT count(*) FROM channels_channelconnection WHERE status = 'F' AND direction = 'I'`).Returns(1)

	testsuite.AssertQuery(t, db, `SELECT count(*) FROM msgs_msg WHERE contact_id = $1 AND msg_type = 'V' AND connection_id = 1 AND status = 'H' AND direction = 'I'`, testdata.Cathy.ID).Returns(5)

	testsuite.AssertQuery(t, db, `SELECT count(*) FROM channels_channellog WHERE connection_id = 1 AND channel_id IS NOT NULL`).Returns(9)

	testsuite.AssertQuery(t, db, `SELECT count(*) FROM msgs_msg WHERE contact_id = $1 AND msg_type = 'V' AND connection_id = 2 
		AND ((status = 'H' AND direction = 'I') OR (status = 'W' AND direction = 'O'))`, testdata.George.ID).Returns(2)

	testsuite.AssertQuery(t, db, `SELECT count(*) FROM channels_channelconnection WHERE status = 'D' AND contact_id = $1`, testdata.George.ID).Returns(1)
}

func TestVonageIVR(t *testing.T) {
	ctx, _, db, rp := testsuite.Get()
	rc := rp.Get()
	defer rc.Close()

	defer func() {
		testsuite.ResetStorage()
		testsuite.Reset()
	}()

	// deactivate our twilio channel
	db.MustExec(`UPDATE channels_channel SET is_active = FALSE WHERE id = $1`, testdata.TwilioChannel.ID)

	// add auth tokens
	db.MustExec(`UPDATE channels_channel SET config = '{"nexmo_app_id": "app_id", "nexmo_app_private_key": "-----BEGIN PRIVATE KEY-----\nMIICdgIBADANBgkqhkiG9w0BAQEFAASCAmAwggJcAgEAAoGBAKNwapOQ6rQJHetP\nHRlJBIh1OsOsUBiXb3rXXE3xpWAxAha0MH+UPRblOko+5T2JqIb+xKf9Vi3oTM3t\nKvffaOPtzKXZauscjq6NGzA3LgeiMy6q19pvkUUOlGYK6+Xfl+B7Xw6+hBMkQuGE\nnUS8nkpR5mK4ne7djIyfHFfMu4ptAgMBAAECgYA+s0PPtMq1osG9oi4xoxeAGikf\nJB3eMUptP+2DYW7mRibc+ueYKhB9lhcUoKhlQUhL8bUUFVZYakP8xD21thmQqnC4\nf63asad0ycteJMLb3r+z26LHuCyOdPg1pyLk3oQ32lVQHBCYathRMcVznxOG16VK\nI8BFfstJTaJu0lK/wQJBANYFGusBiZsJQ3utrQMVPpKmloO2++4q1v6ZR4puDQHx\nTjLjAIgrkYfwTJBLBRZxec0E7TmuVQ9uJ+wMu/+7zaUCQQDDf2xMnQqYknJoKGq+\noAnyC66UqWC5xAnQS32mlnJ632JXA0pf9pb1SXAYExB1p9Dfqd3VAwQDwBsDDgP6\nHD8pAkEA0lscNQZC2TaGtKZk2hXkdcH1SKru/g3vWTkRHxfCAznJUaza1fx0wzdG\nGcES1Bdez0tbW4llI5By/skZc2eE3QJAFl6fOskBbGHde3Oce0F+wdZ6XIJhEgCP\niukIcKZoZQzoiMJUoVRrA5gqnmaYDI5uRRl/y57zt6YksR3KcLUIuQJAd242M/WF\n6YAZat3q/wEeETeQq1wrooew+8lHl05/Nt0cCpV48RGEhJ83pzBm3mnwHf8lTBJH\nx6XroMXsmbnsEw==\n-----END PRIVATE KEY-----", "callback_domain": "localhost:8090"}', role='SRCA' WHERE id = $1`, testdata.VonageChannel.ID)

	// start test server
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
			body, _ := ioutil.ReadAll(r.Body)
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
	}))
	defer ts.Close()

	wg := &sync.WaitGroup{}
	server := web.NewServer(ctx, config.Mailroom, db, rp, testsuite.MediaStorage(), nil, wg)
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
	err := starts.CreateFlowBatches(ctx, db, rp, nil, start)
	assert.NoError(t, err)

	// start our task
	task, err := queue.PopNextTask(rc, queue.HandlerQueue)
	assert.NoError(t, err)
	batch := &models.FlowStartBatch{}
	err = json.Unmarshal(task.Task, batch)
	assert.NoError(t, err)

	// request our call to start
	err = ivr_tasks.HandleFlowStartBatch(ctx, config.Mailroom, db, rp, batch)
	assert.NoError(t, err)

	testsuite.AssertQuery(t, db, `SELECT COUNT(*) FROM channels_channelconnection WHERE contact_id = $1 AND status = $2 AND external_id = $3`,
		testdata.Cathy.ID, models.ConnectionStatusWired, "Call1").Returns(1)

	testsuite.AssertQuery(t, db, `SELECT COUNT(*) FROM channels_channelconnection WHERE contact_id = $1 AND status = $2 AND external_id = $3`,
		testdata.George.ID, models.ConnectionStatusWired, "Call2").Returns(1)

	tcs := []struct {
		label            string
		action           string
		channel          *testdata.Channel
		connectionID     models.ConnectionID
		form             url.Values
		body             string
		expectedStatus   int
		expectedResponse string
		contains         []string
	}{
		{
			label:          "start and prompt",
			action:         "start",
			channel:        testdata.VonageChannel,
			connectionID:   models.ConnectionID(1),
			body:           `{"from":"12482780345","to":"12067799294","uuid":"80c9a606-717e-48b9-ae22-ce00269cbb08","conversation_uuid":"CON-f90649c3-cbf3-42d6-9ab1-01503befac1c"}`,
			expectedStatus: 200,
			contains:       []string{"Hello there. Please enter one or two. Your reference id is 123"},
		},
		{
			label:        "invalid dtmf",
			action:       "resume",
			channel:      testdata.VonageChannel,
			connectionID: models.ConnectionID(1),
			form: url.Values{
				"wait_type": []string{"gather"},
			},
			body:           `{"dtmf":"3","timed_out":false,"uuid":null,"conversation_uuid":"CON-f90649c3-cbf3-42d6-9ab1-01503befac1c","timestamp":"2019-04-01T21:08:54.680Z"}`,
			expectedStatus: 200,
			contains:       []string{"Sorry, that is not one or two, try again."},
		},
		{
			label:        "dtmf 1",
			action:       "resume",
			channel:      testdata.VonageChannel,
			connectionID: models.ConnectionID(1),
			form: url.Values{
				"wait_type": []string{"gather"},
			},
			body:           `{"dtmf":"1","timed_out":false,"uuid":null,"conversation_uuid":"CON-f90649c3-cbf3-42d6-9ab1-01503befac1c","timestamp":"2019-04-01T21:08:54.680Z"}`,
			expectedStatus: 200,
			contains:       []string{"Great! You said One."},
		},
		{
			label:        "dtmf too large",
			action:       "resume",
			channel:      testdata.VonageChannel,
			connectionID: models.ConnectionID(1),
			form: url.Values{
				"wait_type": []string{"gather"},
			},
			body:           `{"dtmf":"101","timed_out":false,"uuid":null,"conversation_uuid":"CON-f90649c3-cbf3-42d6-9ab1-01503befac1c","timestamp":"2019-04-01T21:08:54.680Z"}`,
			expectedStatus: 200,
			contains:       []string{"too big"},
		},
		{
			label:        "dtmf 56",
			action:       "resume",
			channel:      testdata.VonageChannel,
			connectionID: models.ConnectionID(1),
			form: url.Values{
				"wait_type": []string{"gather"},
			},
			body:           `{"dtmf":"56","timed_out":false,"uuid":null,"conversation_uuid":"CON-f90649c3-cbf3-42d6-9ab1-01503befac1c","timestamp":"2019-04-01T21:08:54.680Z"}`,
			expectedStatus: 200,
			contains:       []string{"You picked the number 56"},
		},
		{
			label:        "recording callback",
			action:       "resume",
			channel:      testdata.VonageChannel,
			connectionID: models.ConnectionID(1),
			form: url.Values{
				"wait_type":      []string{"recording_url"},
				"recording_uuid": []string{"0c15f253-8e67-45c8-9980-7d38292edd3c"},
			},
			body:           fmt.Sprintf(`{"recording_url": "%s", "end_time":"2019-04-01T21:08:56.000Z","uuid":"Call1","network":"310260","status":"answered","direction":"outbound","timestamp":"2019-04-01T21:08:56.342Z"}`, ts.URL+"?recording=true"),
			expectedStatus: 200,
			contains:       []string{"inserted recording url"},
		},
		{
			label:        "resume with recording",
			action:       "resume",
			channel:      testdata.VonageChannel,
			connectionID: models.ConnectionID(1),
			form: url.Values{
				"wait_type":      []string{"record"},
				"recording_uuid": []string{"0c15f253-8e67-45c8-9980-7d38292edd3c"},
			},
			body:           `{"end_time":"2019-04-01T21:08:56.000Z","uuid":"Call1","network":"310260","status":"answered","direction":"outbound","timestamp":"2019-04-01T21:08:56.342Z", "recording_url": "http://foo.bar/"}`,
			expectedStatus: 200,
			contains:       []string{"I hope hearing that makes you feel better.", `"action": "conversation"`},
		},
		{
			label:          "transfer answered",
			action:         "status",
			channel:        testdata.VonageChannel,
			connectionID:   models.ConnectionID(1),
			body:           `{"uuid": "Call3", "status": "answered"}`,
			expectedStatus: 200,
			contains:       []string{"updated status for call: Call1 to: answered"},
		},
		{
			label:          "transfer completed",
			action:         "status",
			channel:        testdata.VonageChannel,
			connectionID:   models.ConnectionID(1),
			body:           `{"uuid": "Call3", "duration": "25", "status": "completed"}`,
			expectedStatus: 200,
			contains:       []string{"reconnected call: Call1 to flow with dial status: answered"},
		},
		{
			label:        "transfer callback",
			action:       "resume",
			channel:      testdata.VonageChannel,
			connectionID: models.ConnectionID(1),
			form: url.Values{
				"wait_type":     []string{"dial"},
				"dial_status":   []string{"answered"},
				"dial_duration": []string{"25"},
			},
			expectedStatus: 200,
			contains:       []string{"Great, they answered."},
		},
		{
			label:            "call complete",
			action:           "status",
			channel:          testdata.VonageChannel,
			connectionID:     models.ConnectionID(1),
			body:             `{"end_time":"2019-04-01T21:08:56.000Z","uuid":"Call1","network":"310260","duration":"50","start_time":"2019-04-01T21:08:42.000Z","rate":"0.01270000","price":"0.00296333","from":"12482780345","to":"12067799294","conversation_uuid":"CON-f90649c3-cbf3-42d6-9ab1-01503befac1c","status":"completed","direction":"outbound","timestamp":"2019-04-01T21:08:56.342Z"}`,
			expectedStatus:   200,
			expectedResponse: `{"_message":"status updated: D"}`,
		},
		{
			label:          "new call",
			action:         "start",
			channel:        testdata.VonageChannel,
			connectionID:   models.ConnectionID(2),
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
			label:        "new call dtmf 1",
			action:       "resume",
			channel:      testdata.VonageChannel,
			connectionID: models.ConnectionID(2),
			form: url.Values{
				"wait_type": []string{"gather"},
			},
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
			action:           "status",
			channel:          testdata.VonageChannel,
			connectionID:     models.ConnectionID(2),
			body:             `{"end_time":"2019-04-01T21:08:56.000Z","uuid":"Call2","network":"310260","duration":"50","start_time":"2019-04-01T21:08:42.000Z","rate":"0.01270000","price":"0.00296333","from":"12482780345","to":"12067799294","conversation_uuid":"CON-f90649c3-cbf3-42d6-9ab1-01503befac1c","status":"completed","direction":"outbound","timestamp":"2019-04-01T21:08:56.342Z"}`,
			expectedStatus:   200,
			expectedResponse: `{"_message":"status updated: D"}`,
		},
		{
			label:            "incoming call",
			action:           "incoming",
			channel:          testdata.VonageChannel,
			connectionID:     models.ConnectionID(3),
			body:             `{"from":"12482780345","to":"12067799294","uuid":"Call4","conversation_uuid":"CON-f90649c3-cbf3-42d6-9ab1-01503befac1c"}`,
			expectedStatus:   200,
			expectedResponse: `{"_message":"missed call handled"}`,
		},
		{
			label:            "failed call",
			action:           "status",
			channel:          testdata.VonageChannel,
			connectionID:     models.ConnectionID(3),
			body:             `{"end_time":"2019-04-01T21:08:56.000Z","uuid":"Call4","network":"310260","duration":"50","start_time":"2019-04-01T21:08:42.000Z","rate":"0.01270000","price":"0.00296333","from":"12482780345","to":"12067799294","conversation_uuid":"CON-f90649c3-cbf3-42d6-9ab1-01503befac1c","status":"failed","direction":"outbound","timestamp":"2019-04-01T21:08:56.342Z"}`,
			expectedStatus:   200,
			expectedResponse: `{"_message":"no flow start found, status updated: F"}`,
		},
	}

	for _, tc := range tcs {
		testID := fmt.Sprintf("test '%s' with action '%s'", tc.label, tc.action)

		form := url.Values{
			"action":     []string{tc.action},
			"connection": []string{fmt.Sprintf("%d", tc.connectionID)},
		}
		for k, v := range tc.form {
			form[k] = v
		}
		url := fmt.Sprintf("http://localhost:8090/mr/ivr/c/%s/handle", tc.channel.UUID) + "?" + form.Encode()
		if tc.action == "status" {
			url = fmt.Sprintf("http://localhost:8090/mr/ivr/c/%s/status", tc.channel.UUID)
		}
		if tc.action == "incoming" {
			url = fmt.Sprintf("http://localhost:8090/mr/ivr/c/%s/incoming", tc.channel.UUID)
		}
		req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(tc.body))
		assert.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		assert.Equal(t, tc.expectedStatus, resp.StatusCode, "status code mismatch in %s", testID)

		body, _ := ioutil.ReadAll(resp.Body)

		if tc.expectedResponse != "" {
			test.AssertEqualJSON(t, []byte(tc.expectedResponse), body, "response mismatch in %s", testID)
		}

		for _, needle := range tc.contains {
			if !assert.Containsf(t, string(body), needle, "testcase '%s' does not contain expected body", tc.label) {
				t.FailNow()
			}
		}
	}

	// check our final state of sessions, runs, msgs, connections
	testsuite.AssertQuery(t, db, `SELECT count(*) FROM flows_flowsession WHERE contact_id = $1 AND status = 'C'`, testdata.Cathy.ID).Returns(1)

	testsuite.AssertQuery(t, db, `SELECT count(*) FROM flows_flowrun WHERE contact_id = $1 AND is_active = FALSE`, testdata.Cathy.ID).Returns(1)

	testsuite.AssertQuery(t, db, `SELECT count(*) FROM channels_channelconnection WHERE contact_id = $1 AND status = 'D' AND duration = 50`, testdata.Cathy.ID).Returns(1)

	testsuite.AssertQuery(t, db, `SELECT count(*) FROM msgs_msg WHERE contact_id = $1 AND msg_type = 'V' 
		AND connection_id = 1 AND status = 'W' AND direction = 'O'`, testdata.Cathy.ID).Returns(9)

	testsuite.AssertQuery(t, db, `SELECT count(*) FROM channels_channelconnection WHERE status = 'F' AND direction = 'I'`).Returns(1)

	testsuite.AssertQuery(t, db, `SELECT count(*) FROM msgs_msg WHERE contact_id = $1 AND msg_type = 'V' 
		AND connection_id = 1 AND status = 'H' AND direction = 'I'`, testdata.Cathy.ID).Returns(5)

	testsuite.AssertQuery(t, db, `SELECT count(*) FROM channels_channellog WHERE connection_id = 1 AND channel_id IS NOT NULL`).Returns(10)

	testsuite.AssertQuery(t, db, `SELECT count(*) FROM msgs_msg WHERE contact_id = $1 AND msg_type = 'V' 
		AND connection_id = 2 AND ((status = 'H' AND direction = 'I') OR (status = 'W' AND direction = 'O'))`, testdata.George.ID).Returns(3)

	testsuite.AssertQuery(t, db, `SELECT count(*) FROM channels_channelconnection WHERE status = 'D' AND contact_id = $1`, testdata.George.ID).Returns(1)
}
