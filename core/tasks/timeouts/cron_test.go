package timeouts

import (
	"encoding/json"
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

func TestTimeouts(t *testing.T) {
	testsuite.Reset()
	ctx := testsuite.CTX()
	db := testsuite.DB()
	rp := testsuite.RP()
	rc := testsuite.RC()
	defer rc.Close()

	err := marker.ClearTasks(rc, timeoutLock)
	assert.NoError(t, err)

	// need to create a session that has an expired timeout
	s1TimeoutOn := time.Now()
	testdata.InsertFlowSession(t, db, flows.SessionUUID(uuids.New()), models.Org1, models.CathyID, models.SessionStatusWaiting, &s1TimeoutOn)
	s2TimeoutOn := time.Now().Add(time.Hour * 24)
	testdata.InsertFlowSession(t, db, flows.SessionUUID(uuids.New()), models.Org1, models.GeorgeID, models.SessionStatusWaiting, &s2TimeoutOn)

	time.Sleep(10 * time.Millisecond)

	// schedule our timeouts
	err = timeoutSessions(ctx, db, rp, timeoutLock, "foo")
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
	assert.Equal(t, models.CathyID, eventTask.ContactID)

	// no other
	task, err = queue.PopNextTask(rc, queue.HandlerQueue)
	assert.NoError(t, err)
	assert.Nil(t, task)
}
