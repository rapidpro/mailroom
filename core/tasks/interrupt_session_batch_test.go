package tasks_test

import (
	"fmt"
	"testing"

	valkey "github.com/gomodule/redigo/redis"
	"github.com/nyaruka/gocommon/dbutil/assertdb"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdb"
	"github.com/stretchr/testify/assert"
)

func TestInterruptSessionBatch(t *testing.T) {
	ctx, rt := testsuite.Runtime(t)

	defer testsuite.Reset(t, rt, testsuite.ResetData|testsuite.ResetDynamo|testsuite.ResetValkey)

	s1UUID := testdb.InsertWaitingSession(t, rt, testdb.Org1, testdb.Ann, models.FlowTypeMessaging, nil, testdb.Favorites)
	s2UUID := testdb.InsertWaitingSession(t, rt, testdb.Org1, testdb.Bob, models.FlowTypeMessaging, nil, testdb.Favorites)
	s3UUID := testdb.InsertFlowSession(t, rt, testdb.Cat, models.FlowTypeMessaging, models.SessionStatusCompleted, nil, testdb.Favorites)
	s4UUID := testdb.InsertWaitingSession(t, rt, testdb.Org1, testdb.Cat, models.FlowTypeMessaging, nil, testdb.PickANumber)
	s5UUID := testdb.InsertFlowSession(t, rt, testdb.Dan, models.FlowTypeMessaging, models.SessionStatusInterrupted, nil, testdb.Favorites)

	// queue and perform a task to expire Ann and Cat's sessions
	tasks.Queue(ctx, rt, rt.Queues.Batch, testdb.Org1.ID, &tasks.InterruptSessionBatch{
		Sessions: []models.SessionRef{{UUID: s1UUID, ContactID: testdb.Ann.ID}, {UUID: s4UUID, ContactID: testdb.Cat.ID}},
		Status:   flows.SessionStatusExpired,
	}, false)
	counts := testsuite.FlushTasks(t, rt)

	assert.Equal(t, map[string]int{"interrupt_session_batch": 1}, counts)

	assertdb.Query(t, rt.DB, `SELECT uuid, status FROM flows_flowsession`).Map(map[string]any{
		string(s1UUID): models.SessionStatusExpired,
		string(s2UUID): models.SessionStatusWaiting,
		string(s3UUID): models.SessionStatusCompleted,
		string(s4UUID): models.SessionStatusExpired,
		string(s5UUID): models.SessionStatusInterrupted,
	})
}

func TestInterruptSessionBatchDecrements(t *testing.T) {
	ctx, rt := testsuite.Runtime(t)
	vc := rt.VK.Get()
	defer vc.Close()

	defer testsuite.Reset(t, rt, testsuite.ResetData|testsuite.ResetDynamo|testsuite.ResetValkey)

	s1UUID := testdb.InsertWaitingSession(t, rt, testdb.Org1, testdb.Ann, models.FlowTypeMessaging, nil, testdb.Favorites)
	s2UUID := testdb.InsertWaitingSession(t, rt, testdb.Org1, testdb.Bob, models.FlowTypeMessaging, nil, testdb.Favorites)

	// simulate the counter being set by InterruptFlow with a total of 3 batches
	key := fmt.Sprintf("interrupt_flow_progress:%d", testdb.Favorites.ID)
	vc.Do("SET", key, 3, "EX", 15*60)

	// queue and perform a batch task with FlowID set (as InterruptFlow would create it)
	tasks.Queue(ctx, rt, rt.Queues.Batch, testdb.Org1.ID, &tasks.InterruptSessionBatch{
		Sessions: []models.SessionRef{{UUID: s1UUID, ContactID: testdb.Ann.ID}, {UUID: s2UUID, ContactID: testdb.Bob.ID}},
		Status:   flows.SessionStatusInterrupted,
		FlowID:   testdb.Favorites.ID,
	}, false)
	testsuite.FlushTasks(t, rt)

	// counter should have been decremented by 1 (one batch completed)
	remaining, err := valkey.Int(vc.Do("GET", key))
	assert.NoError(t, err)
	assert.Equal(t, 2, remaining)
}
