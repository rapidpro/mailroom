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

func TestBatchStart(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetAll)

	// create a start object
	testdata.InsertFlowStart(rt, testdata.Org1, testdata.SingleMessage, nil)

	// and our batch object
	contactIDs := []models.ContactID{testdata.Cathy.ID, testdata.Bob.ID}

	tcs := []struct {
		Flow                     models.FlowID
		ExcludeStartedPreviously bool
		ExcludeInAFlow           bool
		Extra                    json.RawMessage
		Msg                      string
		Count                    int
		TotalCount               int
	}{
		{testdata.SingleMessage.ID, false, false, nil, "Hey, how are you?", 2, 2},
		{testdata.SingleMessage.ID, true, false, nil, "Hey, how are you?", 0, 2},
		{testdata.SingleMessage.ID, true, true, nil, "Hey, how are you?", 0, 2},
		{testdata.SingleMessage.ID, false, true, nil, "Hey, how are you?", 2, 4},
		{
			Flow:                     testdata.IncomingExtraFlow.ID,
			ExcludeStartedPreviously: false,
			ExcludeInAFlow:           true,
			Extra:                    json.RawMessage([]byte(`{"name":"Fred", "age":33}`)),
			Msg:                      "Great to meet you Fred. Your age is 33.",
			Count:                    2,
			TotalCount:               2,
		},
	}

	last := time.Now()

	for i, tc := range tcs {
		start := models.NewFlowStart(models.OrgID(1), models.StartTypeManual, models.FlowTypeMessaging, tc.Flow).
			WithContactIDs(contactIDs).
			WithExcludeInAFlow(tc.ExcludeInAFlow).
			WithExcludeStartedPreviously(tc.ExcludeStartedPreviously).
			WithExtra(tc.Extra)
		batch := start.CreateBatch(contactIDs, true, len(contactIDs))

		sessions, err := runner.StartFlowBatch(ctx, rt, batch)
		require.NoError(t, err)
		assert.Equal(t, tc.Count, len(sessions), "%d: unexpected number of sessions created", i)

		assertdb.Query(t, rt.DB,
			`SELECT count(*) FROM flows_flowsession WHERE contact_id = ANY($1) 
			AND status = 'C' AND responded = FALSE AND org_id = 1 AND call_id IS NULL AND output IS NOT NULL AND created_on > $2`, pq.Array(contactIDs), last).
			Returns(tc.Count, "%d: unexpected number of sessions", i)

		assertdb.Query(t, rt.DB,
			`SELECT count(*) FROM flows_flowrun WHERE contact_id = ANY($1) and flow_id = $2
			AND responded = FALSE AND org_id = 1 AND status = 'C'
			AND results IS NOT NULL AND path IS NOT NULL AND session_id IS NOT NULL`, pq.Array(contactIDs), tc.Flow).
			Returns(tc.TotalCount, "%d: unexpected number of runs", i)

		assertdb.Query(t, rt.DB,
			`SELECT count(*) FROM msgs_msg WHERE contact_id = ANY($1) AND text = $2 AND org_id = 1 AND status = 'Q' 
			AND queued_on IS NOT NULL AND direction = 'O' AND msg_type = 'T' AND channel_id = $3`,
			pq.Array(contactIDs), tc.Msg, testdata.TwilioChannel.ID).
			Returns(tc.TotalCount, "%d: unexpected number of messages", i)

		last = time.Now()
	}
}

func TestResume(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetData | testsuite.ResetStorage)

	// write sessions to s3 storage
	rt.Config.SessionStorage = "s3"

	oa, err := models.GetOrgAssetsWithRefresh(ctx, rt, testdata.Org1.ID, models.RefreshOrg)
	require.NoError(t, err)

	flow, err := oa.FlowByID(testdata.Favorites.ID)
	require.NoError(t, err)

	modelContact, flowContact := testdata.Cathy.Load(rt, oa)

	trigger := triggers.NewBuilder(oa.Env(), flow.Reference(), flowContact).Manual().Build()
	sessions, err := runner.StartFlowForContacts(ctx, rt, oa, flow, []*models.Contact{modelContact}, []flows.Trigger{trigger}, nil, true)
	assert.NoError(t, err)
	assert.NotNil(t, sessions)

	assertdb.Query(t, rt.DB,
		`SELECT count(*) FROM flows_flowsession WHERE contact_id = $1 AND current_flow_id = $2
		 AND status = 'W' AND responded = FALSE AND org_id = 1 AND call_id IS NULL AND output IS NULL`, modelContact.ID(), flow.ID()).Returns(1)

	assertdb.Query(t, rt.DB,
		`SELECT count(*) FROM flows_flowrun WHERE contact_id = $1 AND flow_id = $2
		 AND status = 'W' AND responded = FALSE AND org_id = 1`, modelContact.ID(), flow.ID()).Returns(1)

	assertdb.Query(t, rt.DB, `SELECT count(*) FROM msgs_msg WHERE contact_id = $1 AND direction = 'O' AND text like '%favorite color%'`, modelContact.ID()).Returns(1)

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

		assertdb.Query(t, rt.DB,
			`SELECT count(*) FROM flows_flowsession WHERE contact_id = $1
			 AND status = $2 AND responded = TRUE AND org_id = 1 AND call_id IS NULL AND output IS NULL AND output_url IS NOT NULL`, modelContact.ID(), tc.SessionStatus).
			Returns(1, "%d: didn't find expected session", i)

		runQuery := `SELECT count(*) FROM flows_flowrun WHERE contact_id = $1 AND flow_id = $2
		 AND status = $3 AND responded = TRUE AND org_id = 1 AND current_node_uuid IS NOT NULL
		 AND json_array_length(path::json) = $4 AND session_id IS NOT NULL`

		assertdb.Query(t, rt.DB, runQuery, modelContact.ID(), flow.ID(), tc.RunStatus, tc.PathLength).
			Returns(1, "%d: didn't find expected run", i)

		assertdb.Query(t, rt.DB, `SELECT count(*) FROM msgs_msg WHERE contact_id = $1 AND direction = 'O' AND text like $2`, modelContact.ID(), tc.Substring).
			Returns(1, "%d: didn't find expected message", i)
	}
}

func TestStartFlowConcurrency(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetData | testsuite.ResetRedis)

	// check everything works with big ids
	rt.DB.MustExec(`ALTER SEQUENCE flows_flowrun_id_seq RESTART WITH 5000000000;`)
	rt.DB.MustExec(`ALTER SEQUENCE flows_flowsession_id_seq RESTART WITH 5000000000;`)

	// create a flow which has a send_broadcast action which will mean handlers grabbing redis connections
	flow := testdata.InsertFlow(rt, testdata.Org1, testsuite.ReadFile("testdata/broadcast_flow.json"))

	oa := testdata.Org1.Load(rt)

	dbFlow, err := oa.FlowByID(flow.ID)
	require.NoError(t, err)
	flowRef := testdata.Favorites.Reference()

	// create a lot of contacts...
	contacts := make([]*testdata.Contact, 100)
	for i := range contacts {
		contacts[i] = testdata.InsertContact(rt, testdata.Org1, flows.ContactUUID(uuids.New()), "Jim", envs.NilLanguage, models.ContactStatusActive)
	}

	options := &runner.StartOptions{
		ExcludeStartedPreviously: false,
		ExcludeInAFlow:           false,
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

	assertdb.Query(t, rt.DB, `SELECT count(*) FROM flows_flowrun`).Returns(len(contacts))
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM flows_flowsession`).Returns(len(contacts))
}
