package expirations

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	_ "github.com/nyaruka/mailroom/core/handlers"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/queue"
	"github.com/nyaruka/mailroom/core/tasks/handler"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
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
	s1 := testdata.InsertFlowSession(db, testdata.Org1, testdata.Cathy, models.SessionStatusWaiting, nil)
	s2 := testdata.InsertFlowSession(db, testdata.Org1, testdata.George, models.SessionStatusWaiting, nil)
	s3 := testdata.InsertFlowSession(db, testdata.Org1, testdata.Bob, models.SessionStatusWaiting, nil)

	// simple run, no parent
	r1ExpiresOn := time.Now()
	testdata.InsertFlowRun(db, testdata.Org1, s1, testdata.Cathy, testdata.Favorites, models.RunStatusWaiting, "", &r1ExpiresOn)

	// parent run
	r2ExpiresOn := time.Now().Add(time.Hour * 24)
	testdata.InsertFlowRun(db, testdata.Org1, s2, testdata.George, testdata.Favorites, models.RunStatusWaiting, "", &r2ExpiresOn)

	// child run
	r3ExpiresOn := time.Now()
	testdata.InsertFlowRun(db, testdata.Org1, s2, testdata.George, testdata.Favorites, models.RunStatusWaiting, "c4126b59-7a61-4ed5-a2da-c7857580355b", &r3ExpiresOn)

	// run with no session
	r4ExpiresOn := time.Now()
	testdata.InsertFlowRun(db, testdata.Org1, models.SessionID(0), testdata.Cathy, testdata.Favorites, models.RunStatusWaiting, "", &r4ExpiresOn)

	// run with no expires_on
	testdata.InsertFlowRun(db, testdata.Org1, s3, testdata.Bob, testdata.Favorites, models.RunStatusWaiting, "", nil)

	time.Sleep(10 * time.Millisecond)

	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM flows_flowrun WHERE is_active = TRUE AND contact_id = $1;`, []interface{}{testdata.Cathy.ID}, 2)
	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM flows_flowsession WHERE status = 'X' AND contact_id = $1;`, []interface{}{testdata.Cathy.ID}, 0)

	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM flows_flowrun WHERE is_active = TRUE AND contact_id = $1;`, []interface{}{testdata.George.ID}, 2)
	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM flows_flowsession WHERE status = 'X' AND contact_id = $1;`, []interface{}{testdata.George.ID}, 0)

	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM flows_flowrun WHERE is_active = TRUE AND contact_id = $1;`, []interface{}{testdata.Bob.ID}, 1)
	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM flows_flowsession WHERE status = 'X' AND contact_id = $1;`, []interface{}{testdata.Bob.ID}, 0)

	// expire our runs
	err = expireRuns(ctx, db, rp, expirationLock, "foo")
	assert.NoError(t, err)

	// shouldn't have any active runs or sessions
	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM flows_flowrun WHERE is_active = TRUE AND contact_id = $1;`, []interface{}{testdata.Cathy.ID}, 0)
	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM flows_flowsession WHERE status = 'X' AND contact_id = $1;`, []interface{}{testdata.Cathy.ID}, 1)

	// should still have two active runs for George as it needs to continue
	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM flows_flowrun WHERE is_active = TRUE AND contact_id = $1;`, []interface{}{testdata.George.ID}, 2)
	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM flows_flowsession WHERE status = 'X' AND contact_id = $1;`, []interface{}{testdata.George.ID}, 0)

	// runs without expires_on won't be expired
	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM flows_flowrun WHERE is_active = TRUE AND contact_id = $1;`, []interface{}{testdata.Bob.ID}, 1)
	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM flows_flowsession WHERE status = 'X' AND contact_id = $1;`, []interface{}{testdata.Bob.ID}, 0)

	// should have created one task
	task, err := queue.PopNextTask(rc, queue.HandlerQueue)
	assert.NoError(t, err)
	assert.NotNil(t, task)

	// decode the task
	eventTask := &handler.HandleEventTask{}
	err = json.Unmarshal(task.Task, eventTask)
	assert.NoError(t, err)

	// assert its the right contact
	assert.Equal(t, testdata.George.ID, eventTask.ContactID)

	// no other tasks
	task, err = queue.PopNextTask(rc, queue.HandlerQueue)
	assert.NoError(t, err)
	assert.Nil(t, task)
}
