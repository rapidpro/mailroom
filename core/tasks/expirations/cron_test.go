package expirations

import (
	"encoding/json"
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

func TestExpirations(t *testing.T) {
	ctx, rt, db, rp := testsuite.Get()
	rc := rp.Get()
	defer rc.Close()

	defer testsuite.Reset(testsuite.ResetAll)

	err := marker.ClearTasks(rc, expirationLock)
	assert.NoError(t, err)

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

	testsuite.AssertQuery(t, db, `SELECT count(*) FROM flows_flowrun WHERE is_active = TRUE AND contact_id = $1;`, testdata.Cathy.ID).Returns(2)
	testsuite.AssertQuery(t, db, `SELECT count(*) FROM flows_flowsession WHERE status = 'X' AND contact_id = $1;`, testdata.Cathy.ID).Returns(0)

	testsuite.AssertQuery(t, db, `SELECT count(*) FROM flows_flowrun WHERE is_active = TRUE AND contact_id = $1;`, testdata.George.ID).Returns(2)
	testsuite.AssertQuery(t, db, `SELECT count(*) FROM flows_flowsession WHERE status = 'X' AND contact_id = $1;`, testdata.George.ID).Returns(0)

	testsuite.AssertQuery(t, db, `SELECT count(*) FROM flows_flowrun WHERE is_active = TRUE AND contact_id = $1;`, testdata.Bob.ID).Returns(1)
	testsuite.AssertQuery(t, db, `SELECT count(*) FROM flows_flowsession WHERE status = 'X' AND contact_id = $1;`, testdata.Bob.ID).Returns(0)

	// expire our runs
	err = expireRuns(ctx, rt, expirationLock, "foo")
	assert.NoError(t, err)

	// shouldn't have any active runs or sessions
	testsuite.AssertQuery(t, db, `SELECT count(*) FROM flows_flowrun WHERE is_active = TRUE AND contact_id = $1;`, testdata.Cathy.ID).Returns(0)
	testsuite.AssertQuery(t, db, `SELECT count(*) FROM flows_flowsession WHERE status = 'X' AND contact_id = $1;`, testdata.Cathy.ID).Returns(1)

	// should still have two active runs for George as it needs to continue
	testsuite.AssertQuery(t, db, `SELECT count(*) FROM flows_flowrun WHERE is_active = TRUE AND contact_id = $1;`, testdata.George.ID).Returns(2)
	testsuite.AssertQuery(t, db, `SELECT count(*) FROM flows_flowsession WHERE status = 'X' AND contact_id = $1;`, testdata.George.ID).Returns(0)

	// runs without expires_on won't be expired
	testsuite.AssertQuery(t, db, `SELECT count(*) FROM flows_flowrun WHERE is_active = TRUE AND contact_id = $1;`, testdata.Bob.ID).Returns(1)
	testsuite.AssertQuery(t, db, `SELECT count(*) FROM flows_flowsession WHERE status = 'X' AND contact_id = $1;`, testdata.Bob.ID).Returns(0)

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
