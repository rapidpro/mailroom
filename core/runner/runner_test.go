package runner_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/nyaruka/gocommon/dbutil/assertdb"
	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/resumes"
	"github.com/nyaruka/goflow/flows/triggers"
	_ "github.com/nyaruka/mailroom/core/handlers"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/runner"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/nyaruka/mailroom/utils/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFireCampaignEvents(t *testing.T) {
	ctx, rt, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetData | testsuite.ResetRedis)

	campaign := triggers.NewCampaignReference(triggers.CampaignUUID(testdata.RemindersCampaign.UUID), "Doctor Reminders")

	// create event fires for event #3 (Pick A Number, start mode SKIP)
	now := time.Now()
	fire1ID := testdata.InsertEventFire(rt.DB, testdata.Cathy, testdata.RemindersEvent3, now)
	fire2ID := testdata.InsertEventFire(rt.DB, testdata.Bob, testdata.RemindersEvent3, now)
	fire3ID := testdata.InsertEventFire(rt.DB, testdata.Alexandria, testdata.RemindersEvent3, now)

	// create an waiting sessions for Cathy and Alexandria
	testdata.InsertWaitingSession(db, testdata.Org1, testdata.Cathy, models.FlowTypeVoice, testdata.IVRFlow, models.NilConnectionID, time.Now(), time.Now(), false, nil)
	testdata.InsertWaitingSession(db, testdata.Org1, testdata.Alexandria, models.FlowTypeMessaging, testdata.Favorites, models.NilConnectionID, time.Now(), time.Now(), false, nil)

	fires := []*models.EventFire{
		{
			FireID:    fire1ID,
			EventID:   testdata.RemindersEvent3.ID,
			ContactID: testdata.Cathy.ID,
			Scheduled: now,
		},
		{
			FireID:    fire2ID,
			EventID:   testdata.RemindersEvent3.ID,
			ContactID: testdata.Bob.ID,
			Scheduled: now,
		},
		{
			FireID:    fire3ID,
			EventID:   testdata.RemindersEvent3.ID,
			ContactID: testdata.Alexandria.ID,
			Scheduled: now,
		},
	}
	sessions, err := runner.FireCampaignEvents(ctx, rt, testdata.Org1.ID, fires, testdata.PickANumber.UUID, campaign, triggers.CampaignEventUUID(testdata.RemindersEvent3.UUID))
	assert.NoError(t, err)
	assert.Equal(t, 1, len(sessions))

	// cathy has her existing waiting session because event skipped her
	assertdb.Query(t, db, `SELECT count(*) FROM flows_flowsession WHERE contact_id = $1 AND status = 'W'`, testdata.Cathy.ID).Returns(1)
	assertdb.Query(t, db, `SELECT current_flow_id FROM flows_flowsession WHERE contact_id = $1 AND status = 'W'`, testdata.Cathy.ID).Returns(int64(testdata.IVRFlow.ID))
	assertdb.Query(t, db, `SELECT fired_result from campaigns_eventfire WHERE contact_id = $1 AND event_id = $2`, testdata.Cathy.ID, testdata.RemindersEvent3.ID).Returns("S")

	// bob's waiting session is the campaign event because he didn't have a waiting session
	assertdb.Query(t, db, `SELECT count(*) FROM flows_flowsession WHERE contact_id = $1 AND status = 'W'`, testdata.Bob.ID).Returns(1)
	assertdb.Query(t, db, `SELECT current_flow_id FROM flows_flowsession WHERE contact_id = $1 AND status = 'W'`, testdata.Bob.ID).Returns(int64(testdata.PickANumber.ID))
	assertdb.Query(t, db, `SELECT fired_result from campaigns_eventfire WHERE contact_id = $1 AND event_id = $2`, testdata.Bob.ID, testdata.RemindersEvent3.ID).Returns("F")

	// alexandria has her existing waiting session because event skipped her
	assertdb.Query(t, db, `SELECT count(*) FROM flows_flowsession WHERE contact_id = $1 AND status = 'W'`, testdata.Alexandria.ID).Returns(1)
	assertdb.Query(t, db, `SELECT current_flow_id FROM flows_flowsession WHERE contact_id = $1 AND status = 'W'`, testdata.Alexandria.ID).Returns(int64(testdata.Favorites.ID))
	assertdb.Query(t, db, `SELECT fired_result from campaigns_eventfire WHERE contact_id = $1 AND event_id = $2`, testdata.Alexandria.ID, testdata.RemindersEvent3.ID).Returns("S")

	// all event fires fired
	assertdb.Query(t, db, `SELECT count(*) from campaigns_eventfire WHERE fired IS NULL`).Returns(0)

	// create event fires for event #2 (message, start mode PASSIVE)
	now = time.Now()
	fire4ID := testdata.InsertEventFire(rt.DB, testdata.Cathy, testdata.RemindersEvent2, now)
	fire5ID := testdata.InsertEventFire(rt.DB, testdata.Bob, testdata.RemindersEvent2, now)
	fire6ID := testdata.InsertEventFire(rt.DB, testdata.Alexandria, testdata.RemindersEvent2, now)

	fires = []*models.EventFire{
		{
			FireID:    fire4ID,
			EventID:   testdata.RemindersEvent2.ID,
			ContactID: testdata.Cathy.ID,
			Scheduled: now,
		},
		{
			FireID:    fire5ID,
			EventID:   testdata.RemindersEvent2.ID,
			ContactID: testdata.Bob.ID,
			Scheduled: now,
		},
		{
			FireID:    fire6ID,
			EventID:   testdata.RemindersEvent2.ID,
			ContactID: testdata.Alexandria.ID,
			Scheduled: now,
		},
	}

	sessions, err = runner.FireCampaignEvents(ctx, rt, testdata.Org1.ID, fires, testdata.CampaignFlow.UUID, campaign, triggers.CampaignEventUUID(testdata.RemindersEvent2.UUID))
	assert.NoError(t, err)
	assert.Equal(t, 3, len(sessions))

	// cathy still has her existing waiting session and now a completed one
	assertdb.Query(t, db, `SELECT count(*) FROM flows_flowsession WHERE contact_id = $1 AND status = 'W'`, testdata.Cathy.ID).Returns(1)
	assertdb.Query(t, db, `SELECT count(*) FROM flows_flowsession WHERE contact_id = $1 AND status = 'C'`, testdata.Cathy.ID).Returns(1)
	assertdb.Query(t, db, `SELECT fired_result from campaigns_eventfire WHERE contact_id = $1 AND event_id = $2`, testdata.Cathy.ID, testdata.RemindersEvent2.ID).Returns("F")

	// bob still has one waiting session from the previous campaign event and now a completed one
	assertdb.Query(t, db, `SELECT count(*) FROM flows_flowsession WHERE contact_id = $1 AND status = 'W'`, testdata.Bob.ID).Returns(1)
	assertdb.Query(t, db, `SELECT count(*) FROM flows_flowsession WHERE contact_id = $1 AND status = 'C'`, testdata.Bob.ID).Returns(1)
	assertdb.Query(t, db, `SELECT fired_result from campaigns_eventfire WHERE contact_id = $1 AND event_id = $2`, testdata.Bob.ID, testdata.RemindersEvent2.ID).Returns("F")

	// alexandria still has her existing waiting session and now a completed one
	assertdb.Query(t, db, `SELECT count(*) FROM flows_flowsession WHERE contact_id = $1 AND status = 'W'`, testdata.Alexandria.ID).Returns(1)
	assertdb.Query(t, db, `SELECT count(*) FROM flows_flowsession WHERE contact_id = $1 AND status = 'C'`, testdata.Alexandria.ID).Returns(1)
	assertdb.Query(t, db, `SELECT fired_result from campaigns_eventfire WHERE contact_id = $1 AND event_id = $2`, testdata.Alexandria.ID, testdata.RemindersEvent2.ID).Returns("F")

	// create event fires for event #1 (flow, start mode INTERRUPT)
	now = time.Now()
	fire7ID := testdata.InsertEventFire(rt.DB, testdata.Cathy, testdata.RemindersEvent1, now)
	fire8ID := testdata.InsertEventFire(rt.DB, testdata.Bob, testdata.RemindersEvent1, now)
	fire9ID := testdata.InsertEventFire(rt.DB, testdata.Alexandria, testdata.RemindersEvent1, now)

	fires = []*models.EventFire{
		{
			FireID:    fire7ID,
			EventID:   testdata.RemindersEvent1.ID,
			ContactID: testdata.Cathy.ID,
			Scheduled: now,
		},
		{
			FireID:    fire8ID,
			EventID:   testdata.RemindersEvent1.ID,
			ContactID: testdata.Bob.ID,
			Scheduled: now,
		},
		{
			FireID:    fire9ID,
			EventID:   testdata.RemindersEvent1.ID,
			ContactID: testdata.Alexandria.ID,
			Scheduled: now,
		},
	}

	sessions, err = runner.FireCampaignEvents(ctx, rt, testdata.Org1.ID, fires, testdata.Favorites.UUID, campaign, triggers.CampaignEventUUID(testdata.RemindersEvent1.UUID))
	assert.NoError(t, err)
	assert.Equal(t, 3, len(sessions))

	// cathy's existing waiting session should now be interrupted and now she has a waiting session in the Favorites flow
	assertdb.Query(t, db, `SELECT count(*) FROM flows_flowsession WHERE contact_id = $1 AND status = 'I'`, testdata.Cathy.ID).Returns(1)
	assertdb.Query(t, db, `SELECT count(*) FROM flows_flowsession WHERE contact_id = $1 AND status = 'C'`, testdata.Cathy.ID).Returns(1)
	assertdb.Query(t, db, `SELECT count(*) FROM flows_flowsession WHERE contact_id = $1 AND status = 'W' AND current_flow_id = $2`, testdata.Cathy.ID, testdata.Favorites.ID).Returns(1)
	assertdb.Query(t, db, `SELECT fired_result from campaigns_eventfire WHERE contact_id = $1 AND event_id = $2`, testdata.Cathy.ID, testdata.RemindersEvent1.ID).Returns("F")

	// bob's session from the first campaign event should now be interrupted and he has a new waiting session
	assertdb.Query(t, db, `SELECT count(*) FROM flows_flowsession WHERE contact_id = $1 AND status = 'I'`, testdata.Bob.ID).Returns(1)
	assertdb.Query(t, db, `SELECT count(*) FROM flows_flowsession WHERE contact_id = $1 AND status = 'C'`, testdata.Bob.ID).Returns(1)
	assertdb.Query(t, db, `SELECT count(*) FROM flows_flowsession WHERE contact_id = $1 AND status = 'W' AND current_flow_id = $2`, testdata.Bob.ID, testdata.Favorites.ID).Returns(1)
	assertdb.Query(t, db, `SELECT fired_result from campaigns_eventfire WHERE contact_id = $1 AND event_id = $2`, testdata.Bob.ID, testdata.RemindersEvent1.ID).Returns("F")

	// alexandria's existing waiting session should now be interrupted and now she has a waiting session in the Favorites flow
	assertdb.Query(t, db, `SELECT count(*) FROM flows_flowsession WHERE contact_id = $1 AND status = 'I'`, testdata.Alexandria.ID).Returns(1)
	assertdb.Query(t, db, `SELECT count(*) FROM flows_flowsession WHERE contact_id = $1 AND status = 'C'`, testdata.Alexandria.ID).Returns(1)
	assertdb.Query(t, db, `SELECT count(*) FROM flows_flowsession WHERE contact_id = $1 AND status = 'W' AND current_flow_id = $2`, testdata.Alexandria.ID, testdata.Favorites.ID).Returns(1)
	assertdb.Query(t, db, `SELECT fired_result from campaigns_eventfire WHERE contact_id = $1 AND event_id = $2`, testdata.Alexandria.ID, testdata.RemindersEvent1.ID).Returns("F")
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
		Restart       bool
		IncludeActive bool
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

		assertdb.Query(t, db,
			`SELECT count(*) FROM flows_flowsession WHERE contact_id = ANY($1) 
			AND status = 'C' AND responded = FALSE AND org_id = 1 AND connection_id IS NULL AND output IS NOT NULL AND created_on > $2`, pq.Array(contactIDs), last).
			Returns(tc.Count, "%d: unexpected number of sessions", i)

		assertdb.Query(t, db,
			`SELECT count(*) FROM flows_flowrun WHERE contact_id = ANY($1) and flow_id = $2
			AND responded = FALSE AND org_id = 1 AND status = 'C'
			AND results IS NOT NULL AND path IS NOT NULL AND session_id IS NOT NULL`, pq.Array(contactIDs), tc.Flow).
			Returns(tc.TotalCount, "%d: unexpected number of runs", i)

		assertdb.Query(t, db,
			`SELECT count(*) FROM msgs_msg WHERE contact_id = ANY($1) AND text = $2 AND org_id = 1 AND status = 'Q' 
			AND queued_on IS NOT NULL AND direction = 'O' AND topup_id IS NOT NULL AND msg_type = 'F' AND channel_id = $3`,
			pq.Array(contactIDs), tc.Msg, testdata.TwilioChannel.ID).
			Returns(tc.TotalCount, "%d: unexpected number of messages", i)

		last = time.Now()
	}
}

func TestResume(t *testing.T) {
	ctx, rt, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetData | testsuite.ResetStorage)

	// write sessions to s3 storage
	rt.Config.SessionStorage = "s3"

	oa, err := models.GetOrgAssetsWithRefresh(ctx, rt, testdata.Org1.ID, models.RefreshOrg)
	require.NoError(t, err)

	flow, err := oa.FlowByID(testdata.Favorites.ID)
	require.NoError(t, err)

	modelContact, flowContact := testdata.Cathy.Load(db, oa)

	trigger := triggers.NewBuilder(oa.Env(), flow.Reference(), flowContact).Manual().Build()
	sessions, err := runner.StartFlowForContacts(ctx, rt, oa, flow, []*models.Contact{modelContact}, []flows.Trigger{trigger}, nil, true)
	assert.NoError(t, err)
	assert.NotNil(t, sessions)

	assertdb.Query(t, db,
		`SELECT count(*) FROM flows_flowsession WHERE contact_id = $1 AND current_flow_id = $2
		 AND status = 'W' AND responded = FALSE AND org_id = 1 AND connection_id IS NULL AND output IS NULL`, modelContact.ID(), flow.ID()).Returns(1)

	assertdb.Query(t, db,
		`SELECT count(*) FROM flows_flowrun WHERE contact_id = $1 AND flow_id = $2
		 AND status = 'W' AND responded = FALSE AND org_id = 1`, modelContact.ID(), flow.ID()).Returns(1)

	assertdb.Query(t, db, `SELECT count(*) FROM msgs_msg WHERE contact_id = $1 AND direction = 'O' AND text like '%favorite color%'`, modelContact.ID()).Returns(1)

	tcs := []struct {
		Message       string
		SessionStatus models.SessionStatus
		RunStatus     models.RunStatus
		Substring     string
		PathLength    int
	}{
		{"Red", models.SessionStatusWaiting, models.RunStatusWaiting, "%I like Red too%", 4},
		{"Mutzig", models.SessionStatusWaiting, models.RunStatusWaiting, "%they made red Mutzig%", 6},
		{"Luke", models.SessionStatusCompleted, models.RunStatusCompleted, "%Thanks Luke%", 7},
	}

	session := sessions[0]
	for i, tc := range tcs {
		// answer our first question
		msg := flows.NewMsgIn(flows.MsgUUID(uuids.New()), testdata.Cathy.URN, nil, tc.Message, nil)
		msg.SetID(10)
		resume := resumes.NewMsg(oa.Env(), flowContact, msg)

		session, err = runner.ResumeFlow(ctx, rt, oa, session, modelContact, resume, nil)
		assert.NoError(t, err)
		assert.NotNil(t, session)

		assertdb.Query(t, db,
			`SELECT count(*) FROM flows_flowsession WHERE contact_id = $1
			 AND status = $2 AND responded = TRUE AND org_id = 1 AND connection_id IS NULL AND output IS NULL AND output_url IS NOT NULL`, modelContact.ID(), tc.SessionStatus).
			Returns(1, "%d: didn't find expected session", i)

		runQuery := `SELECT count(*) FROM flows_flowrun WHERE contact_id = $1 AND flow_id = $2
		 AND status = $3 AND responded = TRUE AND org_id = 1 AND current_node_uuid IS NOT NULL
		 AND json_array_length(path::json) = $4 AND session_id IS NOT NULL`

		assertdb.Query(t, db, runQuery, modelContact.ID(), flow.ID(), tc.RunStatus, tc.PathLength).
			Returns(1, "%d: didn't find expected run", i)

		assertdb.Query(t, db, `SELECT count(*) FROM msgs_msg WHERE contact_id = $1 AND direction = 'O' AND text like $2`, modelContact.ID(), tc.Substring).
			Returns(1, "%d: didn't find expected message", i)
	}
}

func TestStartFlowConcurrency(t *testing.T) {
	ctx, rt, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetData | testsuite.ResetRedis)

	// check everything works with big ids
	db.MustExec(`ALTER SEQUENCE flows_flowrun_id_seq RESTART WITH 5000000000;`)
	db.MustExec(`ALTER SEQUENCE flows_flowsession_id_seq RESTART WITH 5000000000;`)

	// create a flow which has a send_broadcast action which will mean handlers grabbing redis connections
	flow := testdata.InsertFlow(db, testdata.Org1, testsuite.ReadFile("testdata/broadcast_flow.json"))

	oa := testdata.Org1.Load(rt)

	dbFlow, err := oa.FlowByID(flow.ID)
	require.NoError(t, err)
	flowRef := testdata.Favorites.Reference()

	// create a lot of contacts...
	contacts := make([]*testdata.Contact, 100)
	for i := range contacts {
		contacts[i] = testdata.InsertContact(db, testdata.Org1, flows.ContactUUID(uuids.New()), "Jim", envs.NilLanguage)
	}

	options := &runner.StartOptions{
		ExcludeReruns:  false,
		ExcludeWaiting: false,
		TriggerBuilder: func(contact *flows.Contact) flows.Trigger {
			return triggers.NewBuilder(oa.Env(), flowRef, contact).Manual().Build()
		},
		CommitHook: func(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, oa *models.OrgAssets, session []*models.Session) error {
			return nil
		},
	}

	// start each contact in the flow at the same time...
	test.RunConcurrently(len(contacts), func(i int) {
		sessions, err := runner.StartFlow(ctx, rt, oa, dbFlow, []models.ContactID{contacts[i].ID}, options)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(sessions))
	})

	assertdb.Query(t, db, `SELECT count(*) FROM flows_flowrun`).Returns(len(contacts))
	assertdb.Query(t, db, `SELECT count(*) FROM flows_flowsession`).Returns(len(contacts))
}
