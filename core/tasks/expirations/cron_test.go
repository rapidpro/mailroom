package expirations_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/nyaruka/gocommon/dbutil/assertdb"
	_ "github.com/nyaruka/mailroom/core/handlers"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/queue"
	"github.com/nyaruka/mailroom/core/tasks/expirations"
	"github.com/nyaruka/mailroom/core/tasks/handler"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/stretchr/testify/assert"
)

func TestExpirations(t *testing.T) {
	ctx, rt, db, rp := testsuite.Get()
	rc := rp.Get()
	defer rc.Close()

	defer testsuite.Reset(testsuite.ResetData | testsuite.ResetRedis)

	// create single run session for Cathy, no parent to resume
	s1ID := testdata.InsertWaitingSession(db, testdata.Org1, testdata.Cathy, models.FlowTypeMessaging, testdata.Favorites, models.NilConnectionID, time.Now(), time.Now(), false, nil)
	r1ID := testdata.InsertFlowRun(db, testdata.Org1, s1ID, testdata.Cathy, testdata.Favorites, models.RunStatusWaiting, "", nil)

	// create parent/child session for George, can resume
	s2ID := testdata.InsertWaitingSession(db, testdata.Org1, testdata.George, models.FlowTypeMessaging, testdata.Favorites, models.NilConnectionID, time.Now(), time.Now(), true, nil)
	r2ID := testdata.InsertFlowRun(db, testdata.Org1, s2ID, testdata.George, testdata.Favorites, models.RunStatusActive, "", nil)
	r3ID := testdata.InsertFlowRun(db, testdata.Org1, s2ID, testdata.George, testdata.Favorites, models.RunStatusWaiting, "", nil)

	// create session for Bob with expiration in future
	s3ID := testdata.InsertWaitingSession(db, testdata.Org1, testdata.Bob, models.FlowTypeMessaging, testdata.Favorites, models.NilConnectionID, time.Now(), time.Now().Add(time.Hour), true, nil)
	r4ID := testdata.InsertFlowRun(db, testdata.Org1, s3ID, testdata.Bob, testdata.Favorites, models.RunStatusWaiting, "", nil)

	// create an IVR session for Alexandria
	conn := testdata.InsertConnection(db, testdata.Org1, testdata.TwilioChannel, testdata.Alexandria)
	s4ID := testdata.InsertWaitingSession(db, testdata.Org1, testdata.Alexandria, models.FlowTypeVoice, testdata.IVRFlow, conn, time.Now(), time.Now(), false, nil)
	r5ID := testdata.InsertFlowRun(db, testdata.Org1, s4ID, testdata.Alexandria, testdata.IVRFlow, models.RunStatusWaiting, "", nil)

	time.Sleep(5 * time.Millisecond)

	// expire our sessions...
	err := expirations.HandleWaitExpirations(ctx, rt)
	assert.NoError(t, err)

	// Cathy's session should be expired along with its runs
	assertdb.Query(t, db, `SELECT status FROM flows_flowsession WHERE id = $1;`, s1ID).Columns(map[string]interface{}{"status": "X"})
	assertdb.Query(t, db, `SELECT status FROM flows_flowrun WHERE id = $1;`, r1ID).Columns(map[string]interface{}{"status": "X"})

	// Bob's session and run should be unchanged because it's been queued for resumption
	assertdb.Query(t, db, `SELECT status FROM flows_flowsession WHERE id = $1;`, s2ID).Columns(map[string]interface{}{"status": "W"})
	assertdb.Query(t, db, `SELECT status FROM flows_flowrun WHERE id = $1;`, r2ID).Columns(map[string]interface{}{"status": "A"})
	assertdb.Query(t, db, `SELECT status FROM flows_flowrun WHERE id = $1;`, r3ID).Columns(map[string]interface{}{"status": "W"})

	// George's session and run should be unchanged
	assertdb.Query(t, db, `SELECT status FROM flows_flowsession WHERE id = $1;`, s3ID).Columns(map[string]interface{}{"status": "W"})
	assertdb.Query(t, db, `SELECT status FROM flows_flowrun WHERE id = $1;`, r4ID).Columns(map[string]interface{}{"status": "W"})

	// Alexandria's session and run should be unchanged because IVR expirations are handled separately
	assertdb.Query(t, db, `SELECT status FROM flows_flowsession WHERE id = $1;`, s4ID).Columns(map[string]interface{}{"status": "W"})
	assertdb.Query(t, db, `SELECT status FROM flows_flowrun WHERE id = $1;`, r5ID).Columns(map[string]interface{}{"status": "W"})

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
