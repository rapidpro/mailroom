package timeouts

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

	"github.com/stretchr/testify/assert"
)

func TestTimeouts(t *testing.T) {
	ctx, rt, db, rp := testsuite.Get()
	rc := rp.Get()
	defer rc.Close()

	defer testsuite.Reset(testsuite.ResetData | testsuite.ResetRedis)

	// need to create a session that has an expired timeout
	s1TimeoutOn := time.Now()
	testdata.InsertFlowSession(db, testdata.Org1, testdata.Cathy, models.SessionStatusWaiting, &s1TimeoutOn)
	s2TimeoutOn := time.Now().Add(time.Hour * 24)
	testdata.InsertFlowSession(db, testdata.Org1, testdata.George, models.SessionStatusWaiting, &s2TimeoutOn)

	time.Sleep(10 * time.Millisecond)

	// schedule our timeouts
	err := timeoutSessions(ctx, rt)
	assert.NoError(t, err)

	// should have created one task
	task, err := queue.PopNextTask(rc, queue.HandlerQueue)
	assert.NoError(t, err)
	assert.NotNil(t, task)

	// decode the task
	eventTask := &handler.HandleEventTask{}
	err = json.Unmarshal(task.Task, eventTask)
	assert.NoError(t, err)

	// assert its the right contact
	assert.Equal(t, testdata.Cathy.ID, eventTask.ContactID)

	// no other
	task, err = queue.PopNextTask(rc, queue.HandlerQueue)
	assert.NoError(t, err)
	assert.Nil(t, task)
}
