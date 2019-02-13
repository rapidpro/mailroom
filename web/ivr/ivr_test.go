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
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom"
	"github.com/nyaruka/mailroom/config"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/queue"
	"github.com/nyaruka/mailroom/starts"
	"github.com/nyaruka/mailroom/web"
	"github.com/sirupsen/logrus"

	"github.com/nyaruka/mailroom/testsuite"
	"github.com/stretchr/testify/assert"

	"github.com/jmoiron/sqlx"
	_ "github.com/nyaruka/mailroom/hooks"
	"github.com/nyaruka/mailroom/ivr"
	"github.com/nyaruka/mailroom/ivr/twilio"
)

func insertStart(db *sqlx.DB, uuid utils.UUID, flowID models.FlowID, restartParticipants bool, includeActive bool) models.StartID {
	// note we don't bother with the many to many for contacts and groups in our testing
	var startID models.StartID
	err := db.Get(&startID,
		`INSERT INTO flows_flowstart(is_active, created_on, modified_on, uuid, restart_participants, include_active, 
									 contact_count, status, created_by_id, flow_id, modified_by_id)
							VALUES(TRUE, now(), now(), $1, $2, $3, 0, 'S', 1, $4, 1) RETURNING id;`, uuid, restartParticipants, includeActive, flowID)

	if err != nil {
		panic(err)
	}
	logrus.WithField("start", startID).Info("inserted start")
	return startID
}

func TestIVR(t *testing.T) {
	ctx, db, rp := testsuite.Reset()
	rc := rp.Get()
	defer rc.Close()

	// start test server
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		logrus.WithField("method", r.Method).WithField("url", r.URL.String()).WithField("form", r.Form).Info("test server called")
		if strings.HasSuffix(r.URL.String(), "Calls.json") {
			w.WriteHeader(http.StatusCreated)
			w.Write([]byte(`{"sid": "Call1"}`))
		}
		if strings.HasSuffix(r.URL.String(), "recording.mp3") {
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	twilio.BaseURL = ts.URL
	twilio.IgnoreSignatures = true

	wg := &sync.WaitGroup{}
	server := web.NewServer(ctx, config.Mailroom, db, rp, nil, wg)
	server.Start()
	defer server.Stop()

	// add auth tokens
	db.MustExec(`UPDATE channels_channel SET config = '{"auth_token": "token", "account_sid": "sid", "callback_domain": "localhost:8090"}' WHERE id = $1`, models.TwilioChannelID)

	// create a flow start for cathy
	startID := insertStart(db, utils.NewUUID(), models.IVRFlowID, true, true)
	start := models.NewFlowStart(startID, models.Org1, models.IVRFlow, models.IVRFlowID, nil, []models.ContactID{models.CathyID}, nil, false, true, true, nil, nil)

	// call our master starter
	err := starts.CreateFlowBatches(ctx, db, rp, start)
	assert.NoError(t, err)

	// should have one task in our ivr queue
	task, err := queue.PopNextTask(rc, mailroom.HandlerQueue)
	assert.NoError(t, err)
	batch := &models.FlowStartBatch{}
	err = json.Unmarshal(task.Task, batch)
	assert.NoError(t, err)

	// request our call to start
	err = ivr.HandleFlowStartBatch(ctx, config.Mailroom, db, rp, batch)
	assert.NoError(t, err)
	testsuite.AssertQueryCount(t, db,
		`SELECT COUNT(*) FROM channels_channelconnection WHERE contact_id = $1 AND status = $2 AND external_id = $3`,
		[]interface{}{models.CathyID, models.ConnectionStatusWired, "Call1"},
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
			Contains:     "Hello there. Please enter one or two.",
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
			Action:       "incoming",
			ChannelUUID:  models.TwilioChannelUUID,
			ConnectionID: models.ConnectionID(2),
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
			ConnectionID: models.ConnectionID(2),
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
		6,
	)

	testsuite.AssertQueryCount(t, db,
		`SELECT count(*) FROM channels_channelconnection WHERE status = 'F' AND direction = 'I'`,
		[]interface{}{},
		1,
	)

	testsuite.AssertQueryCount(t, db,
		`SELECT count(*) FROM msgs_msg WHERE contact_id = $1 AND msg_type = 'V' AND connection_id = 1 AND status = 'H' AND direction = 'I'`,
		[]interface{}{models.CathyID},
		4,
	)
}
