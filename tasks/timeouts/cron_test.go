package timeouts

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/nyaruka/goflow/utils/uuids"
	_ "github.com/nyaruka/mailroom/hooks"
	"github.com/nyaruka/mailroom/marker"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/queue"
	"github.com/nyaruka/mailroom/tasks/handler"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/stretchr/testify/assert"
)

func TestTimeouts(t *testing.T) {
	testsuite.Reset()
	ctx := testsuite.CTX()
	rp := testsuite.RP()
	rc := testsuite.RC()
	defer rc.Close()

	err := marker.ClearTasks(rc, timeoutLock)
	assert.NoError(t, err)

	// need to create a session that has an expired timeout
	db := testsuite.DB()
	db.MustExec(`INSERT INTO flows_flowsession(uuid, org_id, status, responded, contact_id, created_on, timeout_on) VALUES ($1, 1, 'W', TRUE, $2, NOW(), NOW());`, uuids.New(), models.CathyID)
	db.MustExec(`INSERT INTO flows_flowsession(uuid, org_id, status, responded, contact_id, created_on, timeout_on) VALUES ($1, 1, 'W', TRUE, $2, NOW(), NOW()+ interval '1' day);`, uuids.New(), models.GeorgeID)
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
