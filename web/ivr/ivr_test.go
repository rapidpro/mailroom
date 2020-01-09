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

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/mailroom/config"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/queue"
	"github.com/nyaruka/mailroom/tasks/starts"
	"github.com/nyaruka/mailroom/web"
	"github.com/sirupsen/logrus"

	"github.com/nyaruka/mailroom/testsuite"
	"github.com/stretchr/testify/assert"

	_ "github.com/nyaruka/mailroom/hooks"
	"github.com/nyaruka/mailroom/ivr"
	"github.com/nyaruka/mailroom/ivr/nexmo"
	"github.com/nyaruka/mailroom/ivr/twiml"
	ivr_tasks "github.com/nyaruka/mailroom/tasks/ivr"
)

func TestTwilioIVR(t *testing.T) {
	ctx, db, rp := testsuite.Reset()
	rc := rp.Get()
	defer rc.Close()

	// start test server
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		logrus.WithField("method", r.Method).WithField("url", r.URL.String()).WithField("form", r.Form).Info("test server called")
		if strings.HasSuffix(r.URL.String(), "Calls.json") {
			to := r.Form.Get("To")
			if to == "+250700000001" {
				w.WriteHeader(http.StatusCreated)
				w.Write([]byte(`{"sid": "Call1"}`))
			} else if to == "+250700000003" {
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
	server := web.NewServer(ctx, config.Mailroom, db, rp, nil, nil, wg)
	server.Start()
	defer server.Stop()

	// add auth tokens
	db.MustExec(`UPDATE channels_channel SET config = '{"auth_token": "token", "account_sid": "sid", "callback_domain": "localhost:8090"}' WHERE id = $1`, models.TwilioChannelID)

	// create a flow start for cathy and george
	parentSummary := json.RawMessage(`{"flow": {"name": "IVR Flow", "uuid": "2f81d0ea-4d75-4843-9371-3f7465311cce"}, "uuid": "8bc73097-ac57-47fb-82e5-184f8ec6dbef", "status": "active", "contact": {"id": 10000, "name": "Cathy", "urns": ["tel:+250700000001?id=10000&priority=50"], "uuid": "6393abc0-283d-4c9b-a1b3-641a035c34bf", "fields": {"gender": {"text": "F"}}, "groups": [{"name": "Doctors", "uuid": "c153e265-f7c9-4539-9dbc-9b358714b638"}], "timezone": "America/Los_Angeles", "created_on": "2019-07-23T09:35:01.439614-07:00"}, "results": {}}`)

	start := models.NewFlowStart(models.Org1, models.IVRFlow, models.IVRFlowID, models.DoRestartParticipants, models.DoIncludeActive).
		WithContactIDs([]models.ContactID{models.CathyID, models.GeorgeID}).
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

	testsuite.AssertQueryCount(t, db,
		`SELECT COUNT(*) FROM channels_channelconnection WHERE contact_id = $1 AND status = $2 AND external_id = $3`,
		[]interface{}{models.CathyID, models.ConnectionStatusWired, "Call1"},
		1,
	)
	testsuite.AssertQueryCount(t, db,
		`SELECT COUNT(*) FROM channels_channelconnection WHERE contact_id = $1 AND status = $2 AND external_id = $3`,
		[]interface{}{models.GeorgeID, models.ConnectionStatusWired, "Call2"},
		1,
	)

	tcs := []struct {
		Action       string
		ChannelUUID  assets.ChannelUUID
		ConnectionID models.ConnectionID
		Form         url.Values
		StatusCode   int
		Contains     string
	}{
		{
			Action:       "start",
			ChannelUUID:  models.TwilioChannelUUID,
			ConnectionID: models.ConnectionID(1),
			Form:         nil,
			StatusCode:   200,
			Contains:     "Hello there. Please enter one or two.  This flow was triggered by Cathy",
		},
		{
			Action:       "resume",
			ChannelUUID:  models.TwilioChannelUUID,
			ConnectionID: models.ConnectionID(1),
			Form: url.Values{
				"CallStatus": []string{"in-progress"},
				"wait_type":  []string{"gather"},
				"timeout":    []string{"true"},
			},
			StatusCode: 200,
			Contains:   "Sorry, that is not one or two, try again.",
		},
		{
			Action:       "resume",
			ChannelUUID:  models.TwilioChannelUUID,
			ConnectionID: models.ConnectionID(1),
			Form: url.Values{
				"CallStatus": []string{"in-progress"},
				"wait_type":  []string{"gather"},
				"Digits":     []string{"1"},
			},
			StatusCode: 200,
			Contains:   "Great! You said One.",
		},
		{
			Action:       "resume",
			ChannelUUID:  models.TwilioChannelUUID,
			ConnectionID: models.ConnectionID(1),
			Form: url.Values{
				"CallStatus": []string{"in-progress"},
				"wait_type":  []string{"gather"},
				"Digits":     []string{"101"},
			},
			StatusCode: 200,
			Contains:   "too big",
		},
		{
			Action:       "resume",
			ChannelUUID:  models.TwilioChannelUUID,
			ConnectionID: models.ConnectionID(1),
			Form: url.Values{
				"CallStatus": []string{"in-progress"},
				"wait_type":  []string{"gather"},
				"Digits":     []string{"56"},
			},
			StatusCode: 200,
			Contains:   "You picked the number 56",
		},
		{
			Action:       "resume",
			ChannelUUID:  models.TwilioChannelUUID,
			ConnectionID: models.ConnectionID(1),
			Form: url.Values{
				"CallStatus": []string{"in-progress"},
				"wait_type":  []string{"record"},
				// no recording as we don't have S3 to back us up, flow just moves forward
			},
			StatusCode: 200,
			Contains:   "I hope hearing that makes you feel better",
		},
		{
			Action:       "status",
			ChannelUUID:  models.TwilioChannelUUID,
			ConnectionID: models.ConnectionID(1),
			Form: url.Values{
				"CallSid":      []string{"Call1"},
				"CallStatus":   []string{"completed"},
				"CallDuration": []string{"50"},
			},
			StatusCode: 200,
			Contains:   "status updated: D",
		},
		{
			Action:       "start",
			ChannelUUID:  models.TwilioChannelUUID,
			ConnectionID: models.ConnectionID(2),
			Form:         nil,
			StatusCode:   200,
			Contains:     "Hello there. Please enter one or two.",
		},
		{
			Action:       "resume",
			ChannelUUID:  models.TwilioChannelUUID,
			ConnectionID: models.ConnectionID(2),
			Form: url.Values{
				"CallStatus": []string{"completed"},
				"wait_type":  []string{"gather"},
				"Digits":     []string{"56"},
			},
			StatusCode: 200,
			Contains:   "<!--call completed-->",
		},
		{
			Action:       "incoming",
			ChannelUUID:  models.TwilioChannelUUID,
			ConnectionID: models.ConnectionID(3),
			Form: url.Values{
				"CallSid":    []string{"Call2"},
				"CallStatus": []string{"completed"},
				"Caller":     []string{"+12065551212"},
			},
			StatusCode: 200,
			Contains:   "missed call handled",
		},
		{
			Action:       "status",
			ChannelUUID:  models.TwilioChannelUUID,
			ConnectionID: models.ConnectionID(3),
			Form: url.Values{
				"CallSid":      []string{"Call2"},
				"CallStatus":   []string{"failed"},
				"CallDuration": []string{"50"},
			},
			StatusCode: 200,
			Contains:   "<!--status updated: F-->",
		},
	}

	for i, tc := range tcs {
		form := url.Values{
			"action":     []string{tc.Action},
			"connection": []string{fmt.Sprintf("%d", tc.ConnectionID)},
		}
		url := fmt.Sprintf("http://localhost:8090/mr/ivr/c/%s/handle", tc.ChannelUUID) + "?" + form.Encode()
		if tc.Action == "status" {
			url = fmt.Sprintf("http://localhost:8090/mr/ivr/c/%s/status", tc.ChannelUUID)
		}
		if tc.Action == "incoming" {
			url = fmt.Sprintf("http://localhost:8090/mr/ivr/c/%s/incoming", tc.ChannelUUID)
		}
		req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(tc.Form.Encode()))
		assert.NoError(t, err)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		resp, err := http.DefaultClient.Do(req)
		assert.Equal(t, tc.StatusCode, resp.StatusCode)

		body, _ := ioutil.ReadAll(resp.Body)
		assert.Containsf(t, string(body), tc.Contains, "%d does not contain expected body", i)
	}

	// check our final state of sessions, runs, msgs, connections
	testsuite.AssertQueryCount(t, db,
		`SELECT count(*) FROM flows_flowsession WHERE contact_id = $1 AND status = 'C'`,
		[]interface{}{models.CathyID},
		1,
	)

	testsuite.AssertQueryCount(t, db,
		`SELECT count(*) FROM flows_flowrun WHERE contact_id = $1 AND is_active = FALSE`,
		[]interface{}{models.CathyID},
		1,
	)

	testsuite.AssertQueryCount(t, db,
		`SELECT count(*) FROM channels_channelconnection WHERE contact_id = $1 AND status = 'D' AND duration = 50`,
		[]interface{}{models.CathyID},
		1,
	)

	testsuite.AssertQueryCount(t, db,
		`SELECT count(*) FROM channels_channelconnection WHERE contact_id = $1 AND status = 'D' AND duration = 50`,
		[]interface{}{models.CathyID},
		1,
	)

	testsuite.AssertQueryCount(t, db,
		`SELECT count(*) FROM msgs_msg WHERE contact_id = $1 AND msg_type = 'V' AND connection_id = 1 AND status = 'W' AND direction = 'O'`,
		[]interface{}{models.CathyID},
		7,
	)

	testsuite.AssertQueryCount(t, db,
		`SELECT count(*) FROM channels_channelconnection WHERE status = 'F' AND direction = 'I'`,
		[]interface{}{},
		1,
	)

	testsuite.AssertQueryCount(t, db,
		`SELECT count(*) FROM msgs_msg WHERE contact_id = $1 AND msg_type = 'V' AND connection_id = 1 AND status = 'H' AND direction = 'I'`,
		[]interface{}{models.CathyID},
		5,
	)

	testsuite.AssertQueryCount(t, db,
		`SELECT count(*) FROM channels_channellog WHERE connection_id = 1 AND channel_id IS NOT NULL`,
		[]interface{}{},
		8,
	)

	testsuite.AssertQueryCount(t, db,
		`SELECT count(*) FROM msgs_msg WHERE contact_id = $1 AND msg_type = 'V' AND connection_id = 2 AND ((status = 'H' AND direction = 'I') OR (status = 'W' AND direction = 'O'))`,
		[]interface{}{models.GeorgeID},
		2,
	)

	testsuite.AssertQueryCount(t, db,
		`SELECT count(*) FROM channels_channelconnection WHERE status = 'D' AND contact_id = $1`,
		[]interface{}{models.GeorgeID},
		1,
	)

}

func TestNexmoIVR(t *testing.T) {
	ctx, db, rp := testsuite.Reset()
	rc := rp.Get()
	defer rc.Close()
	models.FlushCache()

	// deactivate our twilio channel
	db.MustExec(`UPDATE channels_channel SET is_active = FALSE WHERE id = $1`, models.TwilioChannelID)

	// add auth tokens
	db.MustExec(`UPDATE channels_channel SET config = '{"nexmo_app_id": "app_id", "nexmo_app_private_key": "-----BEGIN PRIVATE KEY-----\nMIICdgIBADANBgkqhkiG9w0BAQEFAASCAmAwggJcAgEAAoGBAKNwapOQ6rQJHetP\nHRlJBIh1OsOsUBiXb3rXXE3xpWAxAha0MH+UPRblOko+5T2JqIb+xKf9Vi3oTM3t\nKvffaOPtzKXZauscjq6NGzA3LgeiMy6q19pvkUUOlGYK6+Xfl+B7Xw6+hBMkQuGE\nnUS8nkpR5mK4ne7djIyfHFfMu4ptAgMBAAECgYA+s0PPtMq1osG9oi4xoxeAGikf\nJB3eMUptP+2DYW7mRibc+ueYKhB9lhcUoKhlQUhL8bUUFVZYakP8xD21thmQqnC4\nf63asad0ycteJMLb3r+z26LHuCyOdPg1pyLk3oQ32lVQHBCYathRMcVznxOG16VK\nI8BFfstJTaJu0lK/wQJBANYFGusBiZsJQ3utrQMVPpKmloO2++4q1v6ZR4puDQHx\nTjLjAIgrkYfwTJBLBRZxec0E7TmuVQ9uJ+wMu/+7zaUCQQDDf2xMnQqYknJoKGq+\noAnyC66UqWC5xAnQS32mlnJ632JXA0pf9pb1SXAYExB1p9Dfqd3VAwQDwBsDDgP6\nHD8pAkEA0lscNQZC2TaGtKZk2hXkdcH1SKru/g3vWTkRHxfCAznJUaza1fx0wzdG\nGcES1Bdez0tbW4llI5By/skZc2eE3QJAFl6fOskBbGHde3Oce0F+wdZ6XIJhEgCP\niukIcKZoZQzoiMJUoVRrA5gqnmaYDI5uRRl/y57zt6YksR3KcLUIuQJAd242M/WF\n6YAZat3q/wEeETeQq1wrooew+8lHl05/Nt0cCpV48RGEhJ83pzBm3mnwHf8lTBJH\nx6XroMXsmbnsEw==\n-----END PRIVATE KEY-----", "callback_domain": "localhost:8090"}', role='SRCA' WHERE id = $1`, models.NexmoChannelID)

	// start test server
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("recording") != "" {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte{})
		} else {
			type CallForm struct {
				To []struct {
					Number int64 `json:"number`
				} `json:"to"`
			}
			body, _ := ioutil.ReadAll(r.Body)
			logrus.WithField("method", r.Method).WithField("url", r.URL.String()).WithField("body", string(body)).Info("test server called")
			form := &CallForm{}
			json.Unmarshal(body, form)
			if form.To[0].Number == 250700000001 {
				w.WriteHeader(http.StatusCreated)
				w.Write([]byte(`{ "uuid": "Call1","status": "started","direction": "outbound","conversation_uuid": "Conversation1"}`))
			} else if form.To[0].Number == 250700000003 {
				w.WriteHeader(http.StatusCreated)
				w.Write([]byte(`{ "uuid": "Call2","status": "started","direction": "outbound","conversation_uuid": "Conversation2"}`))
			} else {
				w.WriteHeader(http.StatusNotFound)
			}
		}
	}))
	defer ts.Close()

	wg := &sync.WaitGroup{}
	server := web.NewServer(ctx, config.Mailroom, db, rp, nil, nil, wg)
	server.Start()
	defer server.Stop()

	ivr.WriteAttachments = false
	nexmo.BaseURL = ts.URL
	nexmo.IgnoreSignatures = true

	// create a flow start for cathy and george
	extra := json.RawMessage(`{"ref_id":"123"}`)
	start := models.NewFlowStart(models.Org1, models.IVRFlow, models.IVRFlowID, models.DoRestartParticipants, models.DoIncludeActive).
		WithContactIDs([]models.ContactID{models.CathyID, models.GeorgeID}).
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

	testsuite.AssertQueryCount(t, db,
		`SELECT COUNT(*) FROM channels_channelconnection WHERE contact_id = $1 AND status = $2 AND external_id = $3`,
		[]interface{}{models.CathyID, models.ConnectionStatusWired, "Call1"},
		1,
	)
	testsuite.AssertQueryCount(t, db,
		`SELECT COUNT(*) FROM channels_channelconnection WHERE contact_id = $1 AND status = $2 AND external_id = $3`,
		[]interface{}{models.GeorgeID, models.ConnectionStatusWired, "Call2"},
		1,
	)

	tcs := []struct {
		Action       string
		ChannelUUID  assets.ChannelUUID
		ConnectionID models.ConnectionID
		Form         url.Values
		Body         string
		StatusCode   int
		Contains     string
	}{
		{
			Action:       "start",
			ChannelUUID:  models.NexmoChannelUUID,
			ConnectionID: models.ConnectionID(1),
			Body:         `{"from":"12482780345","to":"12067799294","uuid":"80c9a606-717e-48b9-ae22-ce00269cbb08","conversation_uuid":"CON-f90649c3-cbf3-42d6-9ab1-01503befac1c"}`,
			StatusCode:   200,
			Contains:     "Hello there. Please enter one or two. Your reference id is 123",
		},
		{
			Action:       "resume",
			ChannelUUID:  models.NexmoChannelUUID,
			ConnectionID: models.ConnectionID(1),
			Form: url.Values{
				"wait_type": []string{"gather"},
			},
			Body:       `{"dtmf":"3","timed_out":false,"uuid":null,"conversation_uuid":"CON-f90649c3-cbf3-42d6-9ab1-01503befac1c","timestamp":"2019-04-01T21:08:54.680Z"}`,
			StatusCode: 200,
			Contains:   "Sorry, that is not one or two, try again.",
		},
		{
			Action:       "resume",
			ChannelUUID:  models.NexmoChannelUUID,
			ConnectionID: models.ConnectionID(1),
			Form: url.Values{
				"wait_type": []string{"gather"},
			},
			Body:       `{"dtmf":"1","timed_out":false,"uuid":null,"conversation_uuid":"CON-f90649c3-cbf3-42d6-9ab1-01503befac1c","timestamp":"2019-04-01T21:08:54.680Z"}`,
			StatusCode: 200,
			Contains:   "Great! You said One.",
		},
		{
			Action:       "resume",
			ChannelUUID:  models.NexmoChannelUUID,
			ConnectionID: models.ConnectionID(1),
			Form: url.Values{
				"wait_type": []string{"gather"},
			},
			Body:       `{"dtmf":"101","timed_out":false,"uuid":null,"conversation_uuid":"CON-f90649c3-cbf3-42d6-9ab1-01503befac1c","timestamp":"2019-04-01T21:08:54.680Z"}`,
			StatusCode: 200,
			Contains:   "too big",
		},
		{
			Action:       "resume",
			ChannelUUID:  models.NexmoChannelUUID,
			ConnectionID: models.ConnectionID(1),
			Form: url.Values{
				"wait_type": []string{"gather"},
			},
			Body:       `{"dtmf":"56","timed_out":false,"uuid":null,"conversation_uuid":"CON-f90649c3-cbf3-42d6-9ab1-01503befac1c","timestamp":"2019-04-01T21:08:54.680Z"}`,
			StatusCode: 200,
			Contains:   "You picked the number 56",
		},
		{
			Action:       "resume",
			ChannelUUID:  models.NexmoChannelUUID,
			ConnectionID: models.ConnectionID(1),
			Form: url.Values{
				"wait_type":      []string{"recording_url"},
				"recording_uuid": []string{"0c15f253-8e67-45c8-9980-7d38292edd3c"},
			},
			Body:       fmt.Sprintf(`{"recording_url": "%s", "end_time":"2019-04-01T21:08:56.000Z","uuid":"Call1","network":"310260","status":"answered","direction":"outbound","timestamp":"2019-04-01T21:08:56.342Z"}`, ts.URL+"?recording=true"),
			StatusCode: 200,
			Contains:   "inserted recording url",
		},
		{
			Action:       "resume",
			ChannelUUID:  models.NexmoChannelUUID,
			ConnectionID: models.ConnectionID(1),
			Form: url.Values{
				"wait_type":      []string{"record"},
				"recording_uuid": []string{"0c15f253-8e67-45c8-9980-7d38292edd3c"},
			},
			Body:       `{"end_time":"2019-04-01T21:08:56.000Z","uuid":"Call1","network":"310260","status":"answered","direction":"outbound","timestamp":"2019-04-01T21:08:56.342Z", "recording_url": "http://foo.bar/"}`,
			StatusCode: 200,
			Contains:   "I hope hearing that makes you feel better.",
		},
		{
			Action:       "status",
			ChannelUUID:  models.NexmoChannelUUID,
			ConnectionID: models.ConnectionID(1),
			Body:         `{"end_time":"2019-04-01T21:08:56.000Z","uuid":"Call1","network":"310260","duration":"50","start_time":"2019-04-01T21:08:42.000Z","rate":"0.01270000","price":"0.00296333","from":"12482780345","to":"12067799294","conversation_uuid":"CON-f90649c3-cbf3-42d6-9ab1-01503befac1c","status":"completed","direction":"outbound","timestamp":"2019-04-01T21:08:56.342Z"}`,
			StatusCode:   200,
			Contains:     "status updated: D",
		},
		{
			Action:       "start",
			ChannelUUID:  models.NexmoChannelUUID,
			ConnectionID: models.ConnectionID(2),
			Body:         `{"from":"12482780345","to":"12067799294","uuid":"Call2","conversation_uuid":"CON-f90649c3-cbf3-42d6-9ab1-01503befac1c"}`,
			StatusCode:   200,
			Contains:     "Hello there. Please enter one or two.",
		},
		{
			Action:       "resume",
			ChannelUUID:  models.NexmoChannelUUID,
			ConnectionID: models.ConnectionID(2),
			Form: url.Values{
				"wait_type": []string{"gather"},
			},
			Body:       `{"dtmf":"1","timed_out":false,"uuid":"Call2","conversation_uuid":"CON-f90649c3-cbf3-42d6-9ab1-01503befac1c","timestamp":"2019-04-01T21:08:54.680Z"}`,
			StatusCode: 200,
			Contains:   "Great! You said One.",
		},
		{
			Action:       "status",
			ChannelUUID:  models.NexmoChannelUUID,
			ConnectionID: models.ConnectionID(2),
			Body:         `{"end_time":"2019-04-01T21:08:56.000Z","uuid":"Call2","network":"310260","duration":"50","start_time":"2019-04-01T21:08:42.000Z","rate":"0.01270000","price":"0.00296333","from":"12482780345","to":"12067799294","conversation_uuid":"CON-f90649c3-cbf3-42d6-9ab1-01503befac1c","status":"completed","direction":"outbound","timestamp":"2019-04-01T21:08:56.342Z"}`,
			StatusCode:   200,
			Contains:     "status updated: D",
		},
		{
			Action:       "incoming",
			ChannelUUID:  models.NexmoChannelUUID,
			ConnectionID: models.ConnectionID(3),
			Body:         `{"from":"12482780345","to":"12067799294","uuid":"Call3","conversation_uuid":"CON-f90649c3-cbf3-42d6-9ab1-01503befac1c"}`,
			StatusCode:   200,
			Contains:     "missed call handled",
		},
		{
			Action:       "status",
			ChannelUUID:  models.NexmoChannelUUID,
			ConnectionID: models.ConnectionID(3),
			Body:         `{"end_time":"2019-04-01T21:08:56.000Z","uuid":"Call3","network":"310260","duration":"50","start_time":"2019-04-01T21:08:42.000Z","rate":"0.01270000","price":"0.00296333","from":"12482780345","to":"12067799294","conversation_uuid":"CON-f90649c3-cbf3-42d6-9ab1-01503befac1c","status":"failed","direction":"outbound","timestamp":"2019-04-01T21:08:56.342Z"}`,
			StatusCode:   200,
			Contains:     "status updated: F",
		},
	}

	for i, tc := range tcs {
		form := url.Values{
			"action":     []string{tc.Action},
			"connection": []string{fmt.Sprintf("%d", tc.ConnectionID)},
		}
		for k, v := range tc.Form {
			form[k] = v
		}
		url := fmt.Sprintf("http://localhost:8090/mr/ivr/c/%s/handle", tc.ChannelUUID) + "?" + form.Encode()
		if tc.Action == "status" {
			url = fmt.Sprintf("http://localhost:8090/mr/ivr/c/%s/status", tc.ChannelUUID)
		}
		if tc.Action == "incoming" {
			url = fmt.Sprintf("http://localhost:8090/mr/ivr/c/%s/incoming", tc.ChannelUUID)
		}
		req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(tc.Body))
		assert.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		assert.Equal(t, tc.StatusCode, resp.StatusCode)

		body, _ := ioutil.ReadAll(resp.Body)
		assert.Containsf(t, string(body), tc.Contains, "%d does not contain expected body", i)
	}

	// check our final state of sessions, runs, msgs, connections
	testsuite.AssertQueryCount(t, db,
		`SELECT count(*) FROM flows_flowsession WHERE contact_id = $1 AND status = 'C'`,
		[]interface{}{models.CathyID},
		1,
	)

	testsuite.AssertQueryCount(t, db,
		`SELECT count(*) FROM flows_flowrun WHERE contact_id = $1 AND is_active = FALSE`,
		[]interface{}{models.CathyID},
		1,
	)

	testsuite.AssertQueryCount(t, db,
		`SELECT count(*) FROM channels_channelconnection WHERE contact_id = $1 AND status = 'D' AND duration = 50`,
		[]interface{}{models.CathyID},
		1,
	)

	testsuite.AssertQueryCount(t, db,
		`SELECT count(*) FROM channels_channelconnection WHERE contact_id = $1 AND status = 'D' AND duration = 50`,
		[]interface{}{models.CathyID},
		1,
	)

	testsuite.AssertQueryCount(t, db,
		`SELECT count(*) FROM msgs_msg WHERE contact_id = $1 AND msg_type = 'V' AND connection_id = 1 AND status = 'W' AND direction = 'O'`,
		[]interface{}{models.CathyID},
		8,
	)

	testsuite.AssertQueryCount(t, db,
		`SELECT count(*) FROM channels_channelconnection WHERE status = 'F' AND direction = 'I'`,
		[]interface{}{},
		1,
	)

	testsuite.AssertQueryCount(t, db,
		`SELECT count(*) FROM msgs_msg WHERE contact_id = $1 AND msg_type = 'V' AND connection_id = 1 AND status = 'H' AND direction = 'I'`,
		[]interface{}{models.CathyID},
		5,
	)

	testsuite.AssertQueryCount(t, db,
		`SELECT count(*) FROM channels_channellog WHERE connection_id = 1 AND channel_id IS NOT NULL`,
		[]interface{}{},
		9,
	)

	testsuite.AssertQueryCount(t, db,
		`SELECT count(*) FROM msgs_msg WHERE contact_id = $1 AND msg_type = 'V' AND connection_id = 2 AND ((status = 'H' AND direction = 'I') OR (status = 'W' AND direction = 'O'))`,
		[]interface{}{models.GeorgeID},
		3,
	)

	testsuite.AssertQueryCount(t, db,
		`SELECT count(*) FROM channels_channelconnection WHERE status = 'D' AND contact_id = $1`,
		[]interface{}{models.GeorgeID},
		1,
	)

}
