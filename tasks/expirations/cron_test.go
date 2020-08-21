package expirations

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/nyaruka/goflow/utils/uuids"
	_ "github.com/nyaruka/mailroom/hooks"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/queue"
	"github.com/nyaruka/mailroom/tasks/handler"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/utils/marker"

	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	testsuite.Reset()
	os.Exit(m.Run())
}

func TestExpirations(t *testing.T) {
	ctx := testsuite.CTX()
	rp := testsuite.RP()
	rc := testsuite.RC()
	defer rc.Close()

	err := marker.ClearTasks(rc, expirationLock)
	assert.NoError(t, err)

	// need to create a session that has an expired timeout
	db := testsuite.DB()

	// create a few sessions
	var s1, s2 models.SessionID
	err = db.Get(&s1, `INSERT INTO flows_flowsession(uuid, org_id, status, responded, contact_id, created_on) VALUES ($1, 1, 'W', TRUE, $2, NOW()) RETURNING id;`, uuids.New(), models.CathyID)
	assert.NoError(t, err)

	err = db.Get(&s2, `INSERT INTO flows_flowsession(uuid, org_id, status, responded, contact_id, created_on) VALUES ($1, 1, 'W', TRUE, $2, NOW()) RETURNING id;`, uuids.New(), models.GeorgeID)
	assert.NoError(t, err)

	var r1, r2, r3, r4 models.FlowRunID

	// simple run, no parent
	err = db.Get(&r1, `INSERT INTO flows_flowrun(session_id, status, uuid, is_active, created_on, modified_on, responded, contact_id, flow_id, org_id, expires_on) VALUES($1, $2, 'f240ab19-ed5d-4b51-b934-f2fbb9f8e5ad', TRUE, NOW(), NOW(), TRUE, $3, $4, 1, NOW()) RETURNING id;`, s1, models.RunStatusWaiting, models.CathyID, models.FavoritesFlowID)
	assert.NoError(t, err)

	// parent run
	err = db.Get(&r2, `INSERT INTO flows_flowrun(session_id, status, uuid, is_active, created_on, modified_on, responded, contact_id, flow_id, org_id, expires_on) VALUES($1, $2, 'c4126b59-7a61-4ed5-a2da-c7857580355b', TRUE, NOW(), NOW(), TRUE, $3, $4, 1, NOW() + interval '1' day) RETURNING id;`, s2, models.RunStatusWaiting, models.GeorgeID, models.FavoritesFlowID)
	assert.NoError(t, err)

	// child run
	err = db.Get(&r3, `INSERT INTO flows_flowrun(session_id, status, parent_uuid, uuid, is_active, created_on, modified_on, responded, contact_id, flow_id, org_id, expires_on) VALUES($1, $2, 'c4126b59-7a61-4ed5-a2da-c7857580355b', 'a87b7079-5a3c-4e5f-8a6a-62084807c522', TRUE, NOW(), NOW(), TRUE, $3, $4, 1, NOW()) RETURNING id;`, s2, models.RunStatusWaiting, models.GeorgeID, models.FavoritesFlowID)
	assert.NoError(t, err)

	// run with no session
	err = db.Get(&r4, `INSERT INTO flows_flowrun(session_id, status, uuid, is_active, created_on, modified_on, responded, contact_id, flow_id, org_id, expires_on) VALUES($1, $2, 'd64fac33-933f-44b4-a6e4-53283d07a609', TRUE, NOW(), NOW(), TRUE, $3, $4, 1, NOW()) RETURNING id;`, nil, models.RunStatusWaiting, models.CathyID, models.FavoritesFlowID)
	assert.NoError(t, err)

	time.Sleep(10 * time.Millisecond)

	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM flows_flowrun WHERE is_active = TRUE AND contact_id = $1;`, []interface{}{models.CathyID}, 2)
	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM flows_flowsession WHERE status = 'X' AND contact_id = $1;`, []interface{}{models.CathyID}, 0)

	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM flows_flowrun WHERE is_active = TRUE AND contact_id = $1;`, []interface{}{models.GeorgeID}, 2)
	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM flows_flowsession WHERE status = 'X' AND contact_id = $1;`, []interface{}{models.GeorgeID}, 0)

	// expire our runs
	err = expireRuns(ctx, db, rp, expirationLock, "foo")
	assert.NoError(t, err)

	// shouldn't have any active runs or sessions
	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM flows_flowrun WHERE is_active = TRUE AND contact_id = $1;`, []interface{}{models.CathyID}, 0)
	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM flows_flowsession WHERE status = 'X' AND contact_id = $1;`, []interface{}{models.CathyID}, 1)

	// should still have two active runs for George as it needs to continue
	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM flows_flowrun WHERE is_active = TRUE AND contact_id = $1;`, []interface{}{models.GeorgeID}, 2)
	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM flows_flowsession WHERE status = 'X' AND contact_id = $1;`, []interface{}{models.GeorgeID}, 0)

	// should have created one task
	task, err := queue.PopNextTask(rc, queue.HandlerQueue)
	assert.NoError(t, err)
	assert.NotNil(t, task)

	// decode the task
	eventTask := &handler.HandleEventTask{}
	err = json.Unmarshal(task.Task, eventTask)
	assert.NoError(t, err)

	// assert its the right contact
	assert.Equal(t, models.GeorgeID, eventTask.ContactID)

	// no other tasks
	task, err = queue.PopNextTask(rc, queue.HandlerQueue)
	assert.NoError(t, err)
	assert.Nil(t, task)
}
