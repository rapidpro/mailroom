package runner_test

import (
	"context"
	"testing"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/nyaruka/gocommon/dbutil/assertdb"
	"github.com/nyaruka/gocommon/i18n"
	"github.com/nyaruka/gocommon/uuids"
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

func TestStartFlowBatch(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetAll)

	// create a start object
	start1 := models.NewFlowStart(models.OrgID(1), models.StartTypeManual, testdata.SingleMessage.ID).
		WithContactIDs([]models.ContactID{testdata.Cathy.ID, testdata.Bob.ID, testdata.George.ID, testdata.Alexandria.ID})
	err := models.InsertFlowStarts(ctx, rt.DB, []*models.FlowStart{start1})
	require.NoError(t, err)

	batch1 := start1.CreateBatch([]models.ContactID{testdata.Cathy.ID, testdata.Bob.ID}, models.FlowTypeBackground, false, 4)
	batch2 := start1.CreateBatch([]models.ContactID{testdata.George.ID, testdata.Alexandria.ID}, models.FlowTypeBackground, true, 4)

	// start the first batch...
	sessions, err := runner.StartFlowBatch(ctx, rt, batch1)
	require.NoError(t, err)
	assert.Len(t, sessions, 2)

	assertdb.Query(t, rt.DB, `SELECT count(*) FROM flows_flowsession WHERE contact_id = ANY($1) 
		AND status = 'C' AND responded = FALSE AND org_id = 1 AND call_id IS NULL AND output IS NOT NULL`, pq.Array([]models.ContactID{testdata.Cathy.ID, testdata.Bob.ID})).
		Returns(2)

	assertdb.Query(t, rt.DB, `SELECT count(*) FROM flows_flowrun WHERE contact_id = ANY($1) and flow_id = $2 AND responded = FALSE AND org_id = 1 AND status = 'C'
		AND results IS NOT NULL AND path IS NOT NULL AND session_id IS NOT NULL`, pq.Array([]models.ContactID{testdata.Cathy.ID, testdata.Bob.ID}), testdata.SingleMessage.ID).
		Returns(2)

	assertdb.Query(t, rt.DB, `SELECT count(*) FROM msgs_msg WHERE contact_id = ANY($1) AND text = 'Hey, how are you?' AND org_id = 1 AND status = 'Q' 
		AND queued_on IS NOT NULL AND direction = 'O' AND msg_type = 'T'`, pq.Array([]models.ContactID{testdata.Cathy.ID, testdata.Bob.ID})).
		Returns(2)

	assertdb.Query(t, rt.DB, `SELECT status FROM flows_flowstart WHERE id = $1`, start1.ID).Returns("P")

	// start the second batch...
	sessions, err = runner.StartFlowBatch(ctx, rt, batch2)
	require.NoError(t, err)
	assert.Len(t, sessions, 2)

	assertdb.Query(t, rt.DB, `SELECT status FROM flows_flowstart WHERE id = $1`, start1.ID).Returns("C")

	// create a start object with params
	testdata.InsertFlowStart(rt, testdata.Org1, testdata.IncomingExtraFlow, nil)
	start2 := models.NewFlowStart(models.OrgID(1), models.StartTypeManual, testdata.IncomingExtraFlow.ID).
		WithContactIDs([]models.ContactID{testdata.Cathy.ID}).
		WithParams([]byte(`{"name":"Fred", "age":33}`))
	batch3 := start2.CreateBatch([]models.ContactID{testdata.Cathy.ID}, models.FlowTypeMessaging, true, 1)

	sessions, err = runner.StartFlowBatch(ctx, rt, batch3)
	require.NoError(t, err)
	assert.Len(t, sessions, 1)

	assertdb.Query(t, rt.DB, `SELECT count(*) FROM msgs_msg WHERE text = 'Great to meet you Fred. Your age is 33.'`).Returns(1)
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

	modelContact, flowContact, _ := testdata.Cathy.Load(rt, oa)

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
		contacts[i] = testdata.InsertContact(rt, testdata.Org1, flows.ContactUUID(uuids.New()), "Jim", i18n.NilLanguage, models.ContactStatusActive)
	}

	options := &runner.StartOptions{
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
