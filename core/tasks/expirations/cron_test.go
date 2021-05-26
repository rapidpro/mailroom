package expirations

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/goflow/flows"
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
	s1 := testdata.InsertFlowSession(t, db, flows.SessionUUID(uuids.New()), testdata.Org1.ID, testdata.Cathy.ID, models.SessionStatusWaiting, nil)
	s2 := testdata.InsertFlowSession(t, db, flows.SessionUUID(uuids.New()), testdata.Org1.ID, testdata.George.ID, models.SessionStatusWaiting, nil)
	s3 := testdata.InsertFlowSession(t, db, flows.SessionUUID(uuids.New()), testdata.Org1.ID, testdata.Bob.ID, models.SessionStatusWaiting, nil)

	// simple run, no parent
	r1ExpiresOn := time.Now()
	testdata.InsertFlowRun(t, db, "f240ab19-ed5d-4b51-b934-f2fbb9f8e5ad", testdata.Org1.ID, s1, testdata.Cathy.ID, testdata.Favorites.ID, models.RunStatusWaiting, "", &r1ExpiresOn)

	// parent run
	r2ExpiresOn := time.Now().Add(time.Hour * 24)
	testdata.InsertFlowRun(t, db, "c4126b59-7a61-4ed5-a2da-c7857580355b", testdata.Org1.ID, s2, testdata.George.ID, testdata.Favorites.ID, models.RunStatusWaiting, "", &r2ExpiresOn)

	// child run
	r3ExpiresOn := time.Now()
	testdata.InsertFlowRun(t, db, "a87b7079-5a3c-4e5f-8a6a-62084807c522", testdata.Org1.ID, s2, testdata.George.ID, testdata.Favorites.ID, models.RunStatusWaiting, "c4126b59-7a61-4ed5-a2da-c7857580355b", &r3ExpiresOn)

	// run with no session
	r4ExpiresOn := time.Now()
	testdata.InsertFlowRun(t, db, "d64fac33-933f-44b4-a6e4-53283d07a609", testdata.Org1.ID, models.SessionID(0), testdata.Cathy.ID, testdata.Favorites.ID, models.RunStatusWaiting, "", &r4ExpiresOn)

	// run with no expires_on
	testdata.InsertFlowRun(t, db, "4391fdc4-25ca-4e66-8e05-0cd3a6cbb6a2", testdata.Org1.ID, s3, testdata.Bob.ID, testdata.Favorites.ID, models.RunStatusWaiting, "", nil)

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
