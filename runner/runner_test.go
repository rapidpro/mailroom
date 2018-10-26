package runner

import (
	"testing"
	"time"

	"github.com/lib/pq"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/resumes"
	"github.com/nyaruka/goflow/flows/triggers"
	"github.com/nyaruka/goflow/utils"
	_ "github.com/nyaruka/mailroom/hooks"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/stretchr/testify/assert"
)

func TestCampaignStarts(t *testing.T) {
	testsuite.Reset()
	db := testsuite.DB()
	ctx := testsuite.CTX()
	rp := testsuite.RP()

	// delete our android channel, we want our messages to be sent through courier
	db.MustExec(`DELETE FROM channels_channel where id = 1;`)

	event := triggers.CampaignEvent{
		UUID: "e68f4c70-9db1-44c8-8498-602d6857235e",
		Campaign: triggers.Campaign{
			UUID: "5da68501-61c4-4638-a494-3314a6d5edbd",
			Name: "Doctor Reminders",
		},
	}

	// create our event fires
	now := time.Now()
	db.MustExec(`INSERT INTO campaigns_eventfire(contact_id, event_id, scheduled) VALUES(42,1, $1),(43,1, $1);`, now)

	contacts := []flows.ContactID{42, 43}
	fires := []*models.EventFire{
		&models.EventFire{
			FireID:    1,
			EventID:   1,
			ContactID: 42,
			Scheduled: now,
		},
		&models.EventFire{
			FireID:    2,
			EventID:   1,
			ContactID: 43,
			Scheduled: now,
		},
	}
	sessions, err := FireCampaignEvents(ctx, db, rp, models.OrgID(1), fires, assets.FlowUUID("ab906843-73db-43fb-b44f-c6f4bce4a8fc"), &event)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(sessions))

	testsuite.AssertQueryCount(t, db,
		`SELECT count(*) FROM flows_flowsession WHERE contact_id = ANY($1) 
		 AND status = 'C' AND responded = FALSE AND org_id = 1 AND connection_id IS NULL AND output IS NOT NULL`,
		[]interface{}{pq.Array(contacts)}, 2,
	)

	testsuite.AssertQueryCount(t, db,
		`SELECT count(*) FROM flows_flowrun WHERE contact_id = ANY($1) and flow_id = $2
		 AND is_active = FALSE AND responded = FALSE AND org_id = 1 AND parent_id IS NULL AND exit_type = 'C'
		 AND results IS NOT NULL AND path IS NOT NULL AND events IS NOT NULL
		 AND current_node_uuid = '50b64b9c-a2f1-4d5d-9285-a0188fe8a919'
		 AND session_id IS NOT NULL`,
		[]interface{}{pq.Array(contacts), 31}, 2,
	)

	testsuite.AssertQueryCount(t, db,
		`SELECT count(*) FROM msgs_msg WHERE contact_id = ANY($1) 
		 AND text like '% it is time to consult with your patients.' AND org_id = 1 AND status = 'Q' 
		 AND queued_on IS NOT NULL AND direction = 'O' AND topup_id IS NOT NULL AND msg_type = 'F' AND channel_id = 2`,
		[]interface{}{pq.Array(contacts)}, 2,
	)

	testsuite.AssertQueryCount(t, db,
		`SELECT count(*) from campaigns_eventfire WHERE fired IS NULL`, nil, 0)

	testsuite.AssertQueryCount(t, db,
		`SELECT count(*) from campaigns_eventfire WHERE fired IS NOT NULL AND contact_id IN (42,43) AND event_id = 1`, nil, 2)
}

func TestBatchStart(t *testing.T) {
	testsuite.Reset()
	db := testsuite.DB()
	ctx := testsuite.CTX()
	rp := testsuite.RP()

	// delete our android channel, we want our messages to be sent through courier
	db.MustExec(`DELETE FROM channels_channel where id = 1;`)

	// create a start object
	db.MustExec(
		`INSERT INTO flows_flowstart(is_active, created_on, modified_on, uuid, restart_participants, include_active, contact_count, status, flow_id, created_by_id, modified_by_id)
		 VALUES(TRUE, NOW(), NOW(), $1, TRUE, TRUE, 2, 'P', 31, 1, 1)`, utils.NewUUID())

	// and our batch object
	contactIDs := []flows.ContactID{42, 43}

	tcs := []struct {
		Restart       bool
		IncludeActive bool
		Count         int
		TotalCount    int
	}{
		{true, true, 2, 2},
		{false, true, 0, 2},
		{false, false, 0, 2},
		{true, false, 2, 4},
	}

	for i, tc := range tcs {
		start := models.NewFlowStart(
			models.NewStartID(1), models.OrgID(1), models.FlowID(31),
			nil, contactIDs, tc.Restart, tc.IncludeActive,
		)
		batch := start.CreateBatch(contactIDs)
		batch.SetIsLast(true)

		sessions, err := StartFlowBatch(ctx, db, rp, batch)
		assert.NoError(t, err)
		assert.Equal(t, tc.Count, len(sessions))

		testsuite.AssertQueryCount(t, db,
			`SELECT count(*) FROM flows_flowsession WHERE contact_id = ANY($1) 
			AND status = 'C' AND responded = FALSE AND org_id = 1 AND connection_id IS NULL AND output IS NOT NULL`,
			[]interface{}{pq.Array(contactIDs)}, tc.TotalCount, "%d: unexpected number of sessions", i,
		)

		testsuite.AssertQueryCount(t, db,
			`SELECT count(*) FROM flows_flowrun WHERE contact_id = ANY($1) and flow_id = $2
			AND is_active = FALSE AND responded = FALSE AND org_id = 1 AND parent_id IS NULL AND exit_type = 'C'
			AND results IS NOT NULL AND path IS NOT NULL AND events IS NOT NULL
			AND current_node_uuid = '50b64b9c-a2f1-4d5d-9285-a0188fe8a919'
			AND session_id IS NOT NULL`,
			[]interface{}{pq.Array(contactIDs), 31}, tc.TotalCount, "%d: unexpected number of runs", i,
		)

		testsuite.AssertQueryCount(t, db,
			`SELECT count(*) FROM msgs_msg WHERE contact_id = ANY($1) 
			AND text like '% it is time to consult with your patients.' AND org_id = 1 AND status = 'Q' 
			AND queued_on IS NOT NULL AND direction = 'O' AND topup_id IS NOT NULL AND msg_type = 'F' AND channel_id = 2`,
			[]interface{}{pq.Array(contactIDs)}, tc.TotalCount, "%d: unexpected number of messages", i,
		)
	}
}

func TestContactRuns(t *testing.T) {
	testsuite.Reset()
	db := testsuite.DB()
	ctx := testsuite.CTX()
	rp := testsuite.RP()

	org, err := models.GetOrgAssets(ctx, db, models.OrgID(1))
	assert.NoError(t, err)

	sa, err := models.GetSessionAssets(org)
	assert.NoError(t, err)

	flow, err := org.FlowByID(1)
	assert.NoError(t, err)

	// load our contact
	contacts, err := models.LoadContacts(ctx, db, org, []flows.ContactID{42})
	assert.NoError(t, err)

	contact, err := contacts[0].FlowContact(org, sa)
	assert.NoError(t, err)

	trigger := triggers.NewManualTrigger(org.Env(), contact, flow.FlowReference(), nil, time.Now())
	sessions, err := StartFlowForContacts(ctx, db, rp, org, sa, []flows.Trigger{trigger}, nil, true)
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
		SessionStatus string
		RunActive     bool
		Substring     string
		PathLength    int
		EventLength   int
	}{
		{"Red", "W", true, "%I like Red too%", 4, 3},
		{"Mutzig", "W", true, "%they made red Mutzig%", 6, 5},
		{"Luke", "C", false, "%Thanks Luke%", 7, 7},
	}

	session := sessions[0]
	for i, tc := range tcs {
		// answer our first question
		resume := resumes.NewMsgResume(org.Env(), contact,
			flows.NewMsgIn(flows.MsgUUID(utils.NewUUID()), 10, urns.URN("tel:+250700000001"), nil, tc.Message, nil, ""))

		session, err = ResumeFlow(ctx, db, rp, org, sa, session, resume, nil)
		assert.NoError(t, err)
		assert.NotNil(t, session)

		testsuite.AssertQueryCount(t, db,
			`SELECT count(*) FROM flows_flowsession WHERE contact_id = $1 AND current_flow_id = $2
			 AND status = $3 AND responded = TRUE AND org_id = 1 AND connection_id IS NULL AND output IS NOT NULL`,
			[]interface{}{contact.ID(), flow.ID(), tc.SessionStatus}, 1, "%d: didn't find expected session", i,
		)

		testsuite.AssertQueryCount(t, db,
			`SELECT count(*) FROM flows_flowrun WHERE contact_id = $1 AND flow_id = $2
			 AND is_active = $3 AND responded = TRUE AND org_id = 1 AND current_node_uuid IS NOT NULL
			 AND json_array_length(path::json) = $4 AND json_array_length(events::json) = $5
			 AND session_id IS NOT NULL AND expires_on IS NOT NULL`,
			[]interface{}{contact.ID(), flow.ID(), tc.RunActive, tc.PathLength, tc.EventLength}, 1, "%d: didn't find expected run", i,
		)

		testsuite.AssertQueryCount(t, db,
			`SELECT count(*) FROM msgs_msg WHERE contact_id = $1 AND direction = 'O' AND text like $2`,
			[]interface{}{contact.ID(), tc.Substring}, 1, "%d: didn't find expected message", i,
		)
	}
}
