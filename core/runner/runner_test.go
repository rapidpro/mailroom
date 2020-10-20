package runner

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
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"

	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
)

func TestCampaignStarts(t *testing.T) {
	testsuite.Reset()
	db := testsuite.DB()
	ctx := testsuite.CTX()
	rp := testsuite.RP()

	campaign := triggers.NewCampaignReference(triggers.CampaignUUID(models.DoctorRemindersCampaignUUID), "Doctor Reminders")

	// create our event fires
	now := time.Now()
	db.MustExec(`INSERT INTO campaigns_eventfire(event_id, scheduled, contact_id) VALUES($1, $2, $3),($1, $2, $4),($1, $2, $5);`, models.RemindersEvent2ID, now, models.CathyID, models.BobID, models.AlexandriaID)

	// create an active session for Alexandria to test skipping
	db.MustExec(`INSERT INTO flows_flowsession(uuid, session_type, org_id, contact_id, status, responded, created_on, current_flow_id) VALUES($1, 'M', $2, $3, 'W', FALSE, NOW(), $4);`, uuids.New(), models.Org1, models.AlexandriaID, models.FavoritesFlowID)

	// create an active voice call for Cathy to make sure it doesn't get interrupted or cause skipping
	db.MustExec(`INSERT INTO flows_flowsession(uuid, session_type, org_id, contact_id, status, responded, created_on, current_flow_id) VALUES($1, 'V', $2, $3, 'W', FALSE, NOW(), $4);`, uuids.New(), models.Org1, models.CathyID, models.IVRFlowID)

	// set our event to skip
	db.MustExec(`UPDATE campaigns_campaignevent SET start_mode = 'S' WHERE id= $1`, models.RemindersEvent2ID)

	contacts := []models.ContactID{models.CathyID, models.BobID}
	fires := []*models.EventFire{
		{
			FireID:    1,
			EventID:   models.RemindersEvent2ID,
			ContactID: models.CathyID,
			Scheduled: now,
		},
		{
			FireID:    2,
			EventID:   models.RemindersEvent2ID,
			ContactID: models.BobID,
			Scheduled: now,
		},
		{
			FireID:    3,
			EventID:   models.RemindersEvent2ID,
			ContactID: models.AlexandriaID,
			Scheduled: now,
		},
	}
	sessions, err := FireCampaignEvents(ctx, db, rp, models.Org1, fires, models.CampaignFlowUUID, campaign, "e68f4c70-9db1-44c8-8498-602d6857235e")
	assert.NoError(t, err)
	assert.Equal(t, 2, len(sessions))

	testsuite.AssertQueryCount(t, db,
		`SELECT count(*) FROM flows_flowsession WHERE contact_id = ANY($1) 
		 AND status = 'C' AND responded = FALSE AND org_id = 1 AND connection_id IS NULL AND output IS NOT NULL`,
		[]interface{}{pq.Array(contacts)}, 2,
	)

	testsuite.AssertQueryCount(t, db,
		`SELECT count(*) FROM flows_flowrun WHERE contact_id = ANY($1) and flow_id = $2
		 AND is_active = FALSE AND responded = FALSE AND org_id = 1 AND parent_id IS NULL AND exit_type = 'C' AND status = 'C'
		 AND results IS NOT NULL AND path IS NOT NULL AND events IS NOT NULL
		 AND session_id IS NOT NULL`,
		[]interface{}{pq.Array(contacts), models.CampaignFlowID}, 2,
	)

	testsuite.AssertQueryCount(t, db,
		`SELECT count(*) FROM msgs_msg WHERE contact_id = ANY($1) 
		 AND text like '% it is time to consult with your patients.' AND org_id = 1 AND status = 'Q' 
		 AND queued_on IS NOT NULL AND direction = 'O' AND topup_id IS NOT NULL AND msg_type = 'F' AND channel_id = $2`,
		[]interface{}{pq.Array(contacts), models.TwilioChannelID}, 2,
	)

	testsuite.AssertQueryCount(t, db,
		`SELECT count(*) from campaigns_eventfire WHERE fired IS NULL`, nil, 0)

	testsuite.AssertQueryCount(t, db,
		`SELECT count(*) from campaigns_eventfire WHERE fired IS NOT NULL AND contact_id IN ($1,$2) AND event_id = $3 AND fired_result = 'F'`, []interface{}{models.CathyID, models.BobID, models.RemindersEvent2ID}, 2)

	testsuite.AssertQueryCount(t, db,
		`SELECT count(*) from campaigns_eventfire WHERE fired IS NOT NULL AND contact_id IN ($1) AND event_id = $2 AND fired_result = 'S'`, []interface{}{models.AlexandriaID, models.RemindersEvent2ID}, 1)

	testsuite.AssertQueryCount(t, db,
		`SELECT count(*) from flows_flowsession WHERE status = 'W' AND contact_id = $1 AND session_type = 'V'`, []interface{}{models.CathyID}, 1)
}

func TestBatchStart(t *testing.T) {
	testsuite.Reset()
	db := testsuite.DB()
	ctx := testsuite.CTX()
	rp := testsuite.RP()

	// create a start object
	testdata.InsertFlowStart(t, db, models.Org1, models.SingleMessageFlowID, nil)

	// and our batch object
	contactIDs := []models.ContactID{models.CathyID, models.BobID}

	tcs := []struct {
		Flow          models.FlowID
		Restart       models.RestartParticipants
		IncludeActive models.IncludeActive
		Extra         json.RawMessage
		Msg           string
		Count         int
		TotalCount    int
	}{
		{models.SingleMessageFlowID, true, true, nil, "Hey, how are you?", 2, 2},
		{models.SingleMessageFlowID, false, true, nil, "Hey, how are you?", 0, 2},
		{models.SingleMessageFlowID, false, false, nil, "Hey, how are you?", 0, 2},
		{models.SingleMessageFlowID, true, false, nil, "Hey, how are you?", 2, 4},
		{
			Flow:          models.IncomingExtraFlowID,
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
		start := models.NewFlowStart(models.OrgID(1), models.StartTypeManual, models.MessagingFlow, tc.Flow, tc.Restart, tc.IncludeActive).
			WithContactIDs(contactIDs).
			WithExtra(tc.Extra)
		batch := start.CreateBatch(contactIDs, true, len(contactIDs))

		sessions, err := StartFlowBatch(ctx, db, rp, batch)
		assert.NoError(t, err)
		assert.Equal(t, tc.Count, len(sessions), "%d: unexpected number of sessions created", i)

		testsuite.AssertQueryCount(t, db,
			`SELECT count(*) FROM flows_flowsession WHERE contact_id = ANY($1) 
			AND status = 'C' AND responded = FALSE AND org_id = 1 AND connection_id IS NULL AND output IS NOT NULL AND created_on > $2`,
			[]interface{}{pq.Array(contactIDs), last}, tc.Count, "%d: unexpected number of sessions", i,
		)

		testsuite.AssertQueryCount(t, db,
			`SELECT count(*) FROM flows_flowrun WHERE contact_id = ANY($1) and flow_id = $2
			AND is_active = FALSE AND responded = FALSE AND org_id = 1 AND parent_id IS NULL AND exit_type = 'C' AND status = 'C'
			AND results IS NOT NULL AND path IS NOT NULL AND events IS NOT NULL
			AND session_id IS NOT NULL`,
			[]interface{}{pq.Array(contactIDs), tc.Flow}, tc.TotalCount, "%d: unexpected number of runs", i,
		)

		testsuite.AssertQueryCount(t, db,
			`SELECT count(*) FROM msgs_msg WHERE contact_id = ANY($1) 
			AND text = $2 AND org_id = 1 AND status = 'Q' 
			AND queued_on IS NOT NULL AND direction = 'O' AND topup_id IS NOT NULL AND msg_type = 'F' AND channel_id = $3`,
			[]interface{}{pq.Array(contactIDs), tc.Msg, models.TwilioChannelID}, tc.TotalCount, "%d: unexpected number of messages", i,
		)

		last = time.Now()
	}
}

func TestContactRuns(t *testing.T) {
	testsuite.Reset()
	db := testsuite.DB()
	ctx := testsuite.CTX()
	rp := testsuite.RP()

	oa, err := models.GetOrgAssets(ctx, db, models.Org1)
	assert.NoError(t, err)

	flow, err := oa.FlowByID(models.FavoritesFlowID)
	assert.NoError(t, err)

	// load our contact
	contacts, err := models.LoadContacts(ctx, db, oa, []models.ContactID{models.CathyID})
	assert.NoError(t, err)

	contact, err := contacts[0].FlowContact(oa)
	assert.NoError(t, err)

	trigger := triggers.NewBuilder(oa.Env(), flow.FlowReference(), contact).Manual().Build()
	sessions, err := StartFlowForContacts(ctx, db, rp, oa, flow, []flows.Trigger{trigger}, nil, true)
	assert.NoError(t, err)
	assert.NotNil(t, sessions)

	testsuite.AssertQueryCount(t, db,
		`SELECT count(*) FROM flows_flowsession WHERE contact_id = $1 AND current_flow_id = $2
		 AND status = 'W' AND responded = FALSE AND org_id = 1 AND connection_id IS NULL AND output IS NOT NULL`,
		[]interface{}{contact.ID(), flow.ID()}, 1,
	)

	testsuite.AssertQueryCount(t, db,
		`SELECT count(*) FROM flows_flowrun WHERE contact_id = $1 AND flow_id = $2
		 AND is_active = TRUE AND responded = FALSE AND org_id = 1`,
		[]interface{}{contact.ID(), flow.ID()}, 1,
	)

	testsuite.AssertQueryCount(t, db,
		`SELECT count(*) FROM msgs_msg WHERE contact_id = $1 AND direction = 'O' AND text like '%favorite color%'`,
		[]interface{}{contact.ID()}, 1,
	)

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
		msg := flows.NewMsgIn(flows.MsgUUID(uuids.New()), models.CathyURN, nil, tc.Message, nil)
		msg.SetID(10)
		resume := resumes.NewMsg(oa.Env(), contact, msg)

		session, err = ResumeFlow(ctx, db, rp, oa, session, resume, nil)
		assert.NoError(t, err)
		assert.NotNil(t, session)

		testsuite.AssertQueryCount(t, db,
			`SELECT count(*) FROM flows_flowsession WHERE contact_id = $1 AND current_flow_id = $2
			 AND status = $3 AND responded = TRUE AND org_id = 1 AND connection_id IS NULL AND output IS NOT NULL`,
			[]interface{}{contact.ID(), flow.ID(), tc.SessionStatus}, 1, "%d: didn't find expected session", i,
		)

		runIsActive := tc.RunStatus == models.RunStatusActive || tc.RunStatus == models.RunStatusWaiting

		runQuery := `SELECT count(*) FROM flows_flowrun WHERE contact_id = $1 AND flow_id = $2
		AND status = $3 AND is_active = $4 AND responded = TRUE AND org_id = 1 AND current_node_uuid IS NOT NULL
		AND json_array_length(path::json) = $5 AND json_array_length(events::json) = $6
		AND session_id IS NOT NULL `

		if runIsActive {
			runQuery += `AND expires_on IS NOT NULL`
		} else {
			runQuery += `AND expires_on IS NULL`
		}

		testsuite.AssertQueryCount(t, db,
			runQuery,
			[]interface{}{contact.ID(), flow.ID(), tc.RunStatus, runIsActive, tc.PathLength, tc.EventLength}, 1, "%d: didn't find expected run", i,
		)

		testsuite.AssertQueryCount(t, db,
			`SELECT count(*) FROM msgs_msg WHERE contact_id = $1 AND direction = 'O' AND text like $2`,
			[]interface{}{contact.ID(), tc.Substring}, 1, "%d: didn't find expected message", i,
		)
	}
}
