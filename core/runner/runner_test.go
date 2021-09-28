package runner_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/resumes"
	"github.com/nyaruka/goflow/flows/triggers"
	_ "github.com/nyaruka/mailroom/core/handlers"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/runner"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"

	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCampaignStarts(t *testing.T) {
	ctx, rt, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetAll)

	campaign := triggers.NewCampaignReference(triggers.CampaignUUID(testdata.RemindersCampaign.UUID), "Doctor Reminders")

	// create our event fires
	now := time.Now()
	db.MustExec(`INSERT INTO campaigns_eventfire(event_id, scheduled, contact_id) VALUES($1, $2, $3),($1, $2, $4),($1, $2, $5);`, testdata.RemindersEvent2.ID, now, testdata.Cathy.ID, testdata.Bob.ID, testdata.Alexandria.ID)

	// create an active session for Alexandria to test skipping
	db.MustExec(`INSERT INTO flows_flowsession(uuid, session_type, org_id, contact_id, status, responded, created_on, current_flow_id) VALUES($1, 'M', $2, $3, 'W', FALSE, NOW(), $4);`, uuids.New(), testdata.Org1.ID, testdata.Alexandria.ID, testdata.PickANumber.ID)

	// create an active voice call for Cathy to make sure it doesn't get interrupted or cause skipping
	db.MustExec(`INSERT INTO flows_flowsession(uuid, session_type, org_id, contact_id, status, responded, created_on, current_flow_id) VALUES($1, 'V', $2, $3, 'W', FALSE, NOW(), $4);`, uuids.New(), testdata.Org1.ID, testdata.Cathy.ID, testdata.IVRFlow.ID)

	// set our event to skip
	db.MustExec(`UPDATE campaigns_campaignevent SET start_mode = 'S' WHERE id= $1`, testdata.RemindersEvent2.ID)

	contacts := []models.ContactID{testdata.Cathy.ID, testdata.Bob.ID}
	fires := []*models.EventFire{
		{
			FireID:    1,
			EventID:   testdata.RemindersEvent2.ID,
			ContactID: testdata.Cathy.ID,
			Scheduled: now,
		},
		{
			FireID:    2,
			EventID:   testdata.RemindersEvent2.ID,
			ContactID: testdata.Bob.ID,
			Scheduled: now,
		},
		{
			FireID:    3,
			EventID:   testdata.RemindersEvent2.ID,
			ContactID: testdata.Alexandria.ID,
			Scheduled: now,
		},
	}
	sessions, err := runner.FireCampaignEvents(ctx, rt, testdata.Org1.ID, fires, testdata.CampaignFlow.UUID, campaign, "e68f4c70-9db1-44c8-8498-602d6857235e")
	assert.NoError(t, err)
	assert.Equal(t, 2, len(sessions), "expected only two sessions to be created")

	testsuite.AssertQuery(t, db,
		`SELECT count(*) FROM flows_flowsession WHERE contact_id = ANY($1) AND status = 'C' AND responded = FALSE AND org_id = 1 AND connection_id IS NULL AND output IS NOT NULL`, pq.Array(contacts)).
		Returns(2, "expected only two sessions to be created")

	testsuite.AssertQuery(t, db,
		`SELECT count(*) FROM flows_flowrun WHERE contact_id = ANY($1) and flow_id = $2
		 AND is_active = FALSE AND responded = FALSE AND org_id = 1 AND parent_id IS NULL AND exit_type = 'C' AND status = 'C'
		 AND results IS NOT NULL AND path IS NOT NULL AND events IS NOT NULL
		 AND session_id IS NOT NULL`,
		pq.Array(contacts), testdata.CampaignFlow.ID).Returns(2, "expected only two runs to be created")

	testsuite.AssertQuery(t, db,
		`SELECT count(*) FROM msgs_msg WHERE contact_id = ANY($1) 
		 AND text like '% it is time to consult with your patients.' AND org_id = 1 AND status = 'Q' 
		 AND queued_on IS NOT NULL AND direction = 'O' AND topup_id IS NOT NULL AND msg_type = 'F' AND channel_id = $2`,
		pq.Array(contacts), testdata.TwilioChannel.ID).Returns(2, "expected only two messages to be sent")

	testsuite.AssertQuery(t, db, `SELECT count(*) from campaigns_eventfire WHERE fired IS NULL`).
		Returns(0, "expected all events to be fired")

	testsuite.AssertQuery(t, db,
		`SELECT count(*) from campaigns_eventfire WHERE fired IS NOT NULL AND contact_id IN ($1,$2) AND event_id = $3 AND fired_result = 'F'`, testdata.Cathy.ID, testdata.Bob.ID, testdata.RemindersEvent2.ID).
		Returns(2, "expected bob and cathy to have their event sent to fired")

	testsuite.AssertQuery(t, db,
		`SELECT count(*) from campaigns_eventfire WHERE fired IS NOT NULL AND contact_id IN ($1) AND event_id = $2 AND fired_result = 'S'`, testdata.Alexandria.ID, testdata.RemindersEvent2.ID).
		Returns(1, "expected alexandria to have her event set to skipped")

	testsuite.AssertQuery(t, db,
		`SELECT count(*) from flows_flowsession WHERE status = 'W' AND contact_id = $1 AND session_type = 'V'`, testdata.Cathy.ID).Returns(1)
}

func TestBatchStart(t *testing.T) {
	ctx, rt, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetAll)

	// create a start object
	testdata.InsertFlowStart(db, testdata.Org1, testdata.SingleMessage, nil)

	// and our batch object
	contactIDs := []models.ContactID{testdata.Cathy.ID, testdata.Bob.ID}

	tcs := []struct {
		Flow          models.FlowID
		Restart       models.RestartParticipants
		IncludeActive models.IncludeActive
		Extra         json.RawMessage
		Msg           string
		Count         int
		TotalCount    int
	}{
		{testdata.SingleMessage.ID, true, true, nil, "Hey, how are you?", 2, 2},
		{testdata.SingleMessage.ID, false, true, nil, "Hey, how are you?", 0, 2},
		{testdata.SingleMessage.ID, false, false, nil, "Hey, how are you?", 0, 2},
		{testdata.SingleMessage.ID, true, false, nil, "Hey, how are you?", 2, 4},
		{
			Flow:          testdata.IncomingExtraFlow.ID,
			Restart:       true,
			IncludeActive: false,
			Extra:         json.RawMessage([]byte(`{"name":"Fred", "age":33}`)),
			Msg:           "Great to meet you Fred. Your age is 33.",
			Count:         2,
			TotalCount:    2,
		},
	}

	last := time.Now()

	for i, tc := range tcs {
		start := models.NewFlowStart(models.OrgID(1), models.StartTypeManual, models.FlowTypeMessaging, tc.Flow, tc.Restart, tc.IncludeActive).
			WithContactIDs(contactIDs).
			WithExtra(tc.Extra)
		batch := start.CreateBatch(contactIDs, true, len(contactIDs))

		sessions, err := runner.StartFlowBatch(ctx, rt, batch)
		require.NoError(t, err)
		assert.Equal(t, tc.Count, len(sessions), "%d: unexpected number of sessions created", i)

		testsuite.AssertQuery(t, db,
			`SELECT count(*) FROM flows_flowsession WHERE contact_id = ANY($1) 
			AND status = 'C' AND responded = FALSE AND org_id = 1 AND connection_id IS NULL AND output IS NOT NULL AND created_on > $2`, pq.Array(contactIDs), last).
			Returns(tc.Count, "%d: unexpected number of sessions", i)

		testsuite.AssertQuery(t, db,
			`SELECT count(*) FROM flows_flowrun WHERE contact_id = ANY($1) and flow_id = $2
			AND is_active = FALSE AND responded = FALSE AND org_id = 1 AND parent_id IS NULL AND exit_type = 'C' AND status = 'C'
			AND results IS NOT NULL AND path IS NOT NULL AND events IS NOT NULL
			AND session_id IS NOT NULL`, pq.Array(contactIDs), tc.Flow).
			Returns(tc.TotalCount, "%d: unexpected number of runs", i)

		testsuite.AssertQuery(t, db,
			`SELECT count(*) FROM msgs_msg WHERE contact_id = ANY($1) AND text = $2 AND org_id = 1 AND status = 'Q' 
			AND queued_on IS NOT NULL AND direction = 'O' AND topup_id IS NOT NULL AND msg_type = 'F' AND channel_id = $3`,
			pq.Array(contactIDs), tc.Msg, testdata.TwilioChannel.ID).
			Returns(tc.TotalCount, "%d: unexpected number of messages", i)

		last = time.Now()
	}
}

func TestResume(t *testing.T) {
	ctx, rt, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetAll)

	// write sessions to s3 storage
	db.MustExec(`UPDATE orgs_org set config = '{"session_storage_mode": "s3"}' WHERE id = 1`)

	oa, err := models.GetOrgAssetsWithRefresh(ctx, rt, testdata.Org1.ID, models.RefreshOrg)
	require.NoError(t, err)

	flow, err := oa.FlowByID(testdata.Favorites.ID)
	require.NoError(t, err)

	_, contact := testdata.Cathy.Load(db, oa)

	trigger := triggers.NewBuilder(oa.Env(), flow.FlowReference(), contact).Manual().Build()
	sessions, err := runner.StartFlowForContacts(ctx, rt, oa, flow, []flows.Trigger{trigger}, nil, true)
	assert.NoError(t, err)
	assert.NotNil(t, sessions)

	testsuite.AssertQuery(t, db,
		`SELECT count(*) FROM flows_flowsession WHERE contact_id = $1 AND current_flow_id = $2
		 AND status = 'W' AND responded = FALSE AND org_id = 1 AND connection_id IS NULL AND output IS NULL`, contact.ID(), flow.ID()).Returns(1)

	testsuite.AssertQuery(t, db,
		`SELECT count(*) FROM flows_flowrun WHERE contact_id = $1 AND flow_id = $2
		 AND is_active = TRUE AND responded = FALSE AND org_id = 1`, contact.ID(), flow.ID()).Returns(1)

	testsuite.AssertQuery(t, db, `SELECT count(*) FROM msgs_msg WHERE contact_id = $1 AND direction = 'O' AND text like '%favorite color%'`, contact.ID()).Returns(1)

	tcs := []struct {
		Message       string
		SessionStatus flows.SessionStatus
		RunStatus     models.RunStatus
		Substring     string
		PathLength    int
		EventLength   int
	}{
		{"Red", models.SessionStatusWaiting, models.RunStatusWaiting, "%I like Red too%", 4, 3},
		{"Mutzig", models.SessionStatusWaiting, models.RunStatusWaiting, "%they made red Mutzig%", 6, 5},
		{"Luke", models.SessionStatusCompleted, models.RunStatusCompleted, "%Thanks Luke%", 7, 7},
	}

	session := sessions[0]
	for i, tc := range tcs {
		// answer our first question
		msg := flows.NewMsgIn(flows.MsgUUID(uuids.New()), testdata.Cathy.URN, nil, tc.Message, nil)
		msg.SetID(10)
		resume := resumes.NewMsg(oa.Env(), contact, msg)

		session, err = runner.ResumeFlow(ctx, rt, oa, session, resume, nil)
		assert.NoError(t, err)
		assert.NotNil(t, session)

		testsuite.AssertQuery(t, db,
			`SELECT count(*) FROM flows_flowsession WHERE contact_id = $1 AND current_flow_id = $2
			 AND status = $3 AND responded = TRUE AND org_id = 1 AND connection_id IS NULL AND output IS NULL AND output_url IS NOT NULL`, contact.ID(), flow.ID(), tc.SessionStatus).
			Returns(1, "%d: didn't find expected session", i)

		runIsActive := tc.RunStatus == models.RunStatusActive || tc.RunStatus == models.RunStatusWaiting

		runQuery := `SELECT count(*) FROM flows_flowrun WHERE contact_id = $1 AND flow_id = $2
		 AND status = $3 AND is_active = $4 AND responded = TRUE AND org_id = 1 AND current_node_uuid IS NOT NULL
		 AND json_array_length(path::json) = $5 AND json_array_length(events::json) = $6 
		 AND session_id IS NOT NULL`

		if runIsActive {
			runQuery += ` AND expires_on IS NOT NULL`
		} else {
			runQuery += ` AND expires_on IS NULL`
		}

		testsuite.AssertQuery(t, db, runQuery, contact.ID(), flow.ID(), tc.RunStatus, runIsActive, tc.PathLength, tc.EventLength).
			Returns(1, "%d: didn't find expected run", i)

		testsuite.AssertQuery(t, db, `SELECT count(*) FROM msgs_msg WHERE contact_id = $1 AND direction = 'O' AND text like $2`, contact.ID(), tc.Substring).
			Returns(1, "%d: didn't find expected message", i)
	}
}
