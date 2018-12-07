package timeouts

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom"
	"github.com/nyaruka/mailroom/handler"
	_ "github.com/nyaruka/mailroom/hooks"
	"github.com/nyaruka/mailroom/marker"
	"github.com/nyaruka/mailroom/queue"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	testsuite.Reset()
	os.Exit(m.Run())
}

func TestTimeouts(t *testing.T) {
	ctx := testsuite.CTX()
	rp := testsuite.RP()
	rc := testsuite.RC()
	defer rc.Close()

	err := marker.ClearTasks(rc, timeoutLock)
	assert.NoError(t, err)

	// need to create a session that has an expired timeout
	db := testsuite.DB()
	db.MustExec(`UPDATE orgs_org SET flow_server_enabled=TRUE WHERE id = 1;`)
	db.MustExec(`INSERT INTO flows_flowsession(org_id, status, responded, contact_id, created_on, timeout_on) VALUES (1, 'W', TRUE, 42, NOW(), NOW());`)
	db.MustExec(`INSERT INTO flows_flowsession(org_id, status, responded, contact_id, created_on, timeout_on) VALUES (1, 'W', TRUE, 43, NOW(), NOW()+ interval '1' day);`)
	time.Sleep(10 * time.Millisecond)

	// schedule our timeouts
	err = timeoutSessions(ctx, db, rp, timeoutLock, "foo")
	assert.NoError(t, err)

	// should have created one task
	task, err := queue.PopNextTask(rc, mailroom.HandlerQueue)
	assert.NoError(t, err)
	assert.NotNil(t, task)

	// decode the task
	eventTask := &handler.HandleEventTask{}
	err = json.Unmarshal(task.Task, eventTask)
	assert.NoError(t, err)

	// assert its the right contact
	assert.Equal(t, flows.ContactID(42), eventTask.ContactID)

	// no other
	task, err = queue.PopNextTask(rc, mailroom.HandlerQueue)
	assert.NoError(t, err)
	assert.Nil(t, task)
}
