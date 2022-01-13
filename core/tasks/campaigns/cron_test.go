package campaigns_test

import (
	"encoding/json"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/nyaruka/gocommon/dbutil/assertdb"
	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/goflow/flows"
	_ "github.com/nyaruka/mailroom/core/handlers"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/queue"
	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/nyaruka/mailroom/core/tasks/campaigns"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueueEventFires(t *testing.T) {
	ctx, rt, db, rp := testsuite.Get()
	rc := rp.Get()
	defer rc.Close()

	defer testsuite.Reset(testsuite.ResetData | testsuite.ResetRedis)

	org2Campaign := testdata.InsertCampaign(db, testdata.Org2, "Org 2", testdata.DoctorsGroup)
	org2CampaignEvent := testdata.InsertCampaignFlowEvent(db, org2Campaign, testdata.Org2Favorites, testdata.AgeField, 1, "D")

	// try with zero fires
	err := campaigns.QueueEventFires(ctx, rt)
	assert.NoError(t, err)

	assertFireTasks(t, rp, testdata.Org1, [][]models.FireID{})
	assertFireTasks(t, rp, testdata.Org2, [][]models.FireID{})

	// create event fires due now for 2 contacts and in the future for another contact
	fire1ID := testdata.InsertEventFire(rt.DB, testdata.Cathy, testdata.RemindersEvent1, time.Now().Add(-time.Minute))
	fire2ID := testdata.InsertEventFire(rt.DB, testdata.George, testdata.RemindersEvent1, time.Now().Add(-time.Minute))
	fire3ID := testdata.InsertEventFire(rt.DB, testdata.Org2Contact, org2CampaignEvent, time.Now().Add(-time.Minute))
	fire4ID := testdata.InsertEventFire(rt.DB, testdata.Alexandria, testdata.RemindersEvent2, time.Now().Add(-time.Minute))
	testdata.InsertEventFire(rt.DB, testdata.Alexandria, testdata.RemindersEvent1, time.Now().Add(time.Hour*24)) // in future

	// schedule our campaign to be started
	err = campaigns.QueueEventFires(ctx, rt)
	assert.NoError(t, err)

	assertFireTasks(t, rp, testdata.Org1, [][]models.FireID{{fire1ID, fire2ID}, {fire4ID}})
	assertFireTasks(t, rp, testdata.Org2, [][]models.FireID{{fire3ID}})

	// running again won't double add those fires
	err = campaigns.QueueEventFires(ctx, rt)
	assert.NoError(t, err)

	assertFireTasks(t, rp, testdata.Org1, [][]models.FireID{{fire1ID, fire2ID}, {fire4ID}})
	assertFireTasks(t, rp, testdata.Org2, [][]models.FireID{{fire3ID}})

	// clear queued tasks
	rc.Do("DEL", "batch:active")
	rc.Do("DEL", "batch:1")

	// add 110 scheduled event fires to test batch limits
	for i := 0; i < 110; i++ {
		contact := testdata.InsertContact(db, testdata.Org1, flows.ContactUUID(uuids.New()), fmt.Sprintf("Jim %d", i), envs.NilLanguage)
		testdata.InsertEventFire(rt.DB, contact, testdata.RemindersEvent1, time.Now().Add(-time.Minute))
	}

	err = campaigns.QueueEventFires(ctx, rt)
	assert.NoError(t, err)

	queuedTasks := testsuite.CurrentOrgTasks(t, rp)
	org1Tasks := queuedTasks[testdata.Org1.ID]

	assert.Equal(t, 2, len(org1Tasks))

	tk1 := struct {
		FireIDs []models.FireID `json:"fire_ids"`
	}{}
	jsonx.MustUnmarshal(org1Tasks[0].Task, &tk1)
	tk2 := struct {
		FireIDs []models.FireID `json:"fire_ids"`
	}{}
	jsonx.MustUnmarshal(org1Tasks[1].Task, &tk2)

	assert.Equal(t, 100, len(tk1.FireIDs))
	assert.Equal(t, 10, len(tk2.FireIDs))
}
func TestFireCampaignEvents(t *testing.T) {
	ctx, rt, db, rp := testsuite.Get()
	rc := rp.Get()
	defer rc.Close()

	defer testsuite.Reset(testsuite.ResetData | testsuite.ResetRedis)

	// create due fires for Cathy and George
	testdata.InsertEventFire(rt.DB, testdata.Cathy, testdata.RemindersEvent1, time.Now().Add(-time.Minute))
	testdata.InsertEventFire(rt.DB, testdata.George, testdata.RemindersEvent1, time.Now().Add(-time.Minute))

	// queue the event task
	err := campaigns.QueueEventFires(ctx, rt)
	assert.NoError(t, err)

	// then actually work on the event
	task, err := queue.PopNextTask(rc, queue.BatchQueue)
	assert.NoError(t, err)
	assert.NotNil(t, task)

	typedTask, err := tasks.ReadTask(task.Type, task.Task)
	require.NoError(t, err)

	// work on that task
	err = typedTask.Perform(ctx, rt, models.OrgID(task.OrgID))
	assert.NoError(t, err)

	// should now have a flow run for that contact and flow
	assertdb.Query(t, db, `SELECT COUNT(*) from flows_flowrun WHERE contact_id = $1 AND flow_id = $2;`, testdata.Cathy.ID, testdata.Favorites.ID).Returns(1)
	assertdb.Query(t, db, `SELECT COUNT(*) from flows_flowrun WHERE contact_id = $1 AND flow_id = $2;`, testdata.George.ID, testdata.Favorites.ID).Returns(1)
}

func TestIVRCampaigns(t *testing.T) {
	ctx, rt, db, rp := testsuite.Get()
	rc := rp.Get()
	defer rc.Close()

	defer testsuite.Reset(testsuite.ResetAll)

	// turn a campaign event into an IVR flow event
	rt.DB.MustExec(`UPDATE campaigns_campaignevent SET flow_id = $1 WHERE id = $2`, testdata.IVRFlow.ID, testdata.RemindersEvent1.ID)

	testdata.InsertEventFire(rt.DB, testdata.Cathy, testdata.RemindersEvent1, time.Now().Add(-time.Minute))
	testdata.InsertEventFire(rt.DB, testdata.George, testdata.RemindersEvent1, time.Now().Add(-time.Minute))

	// schedule our campaign to be started
	err := campaigns.QueueEventFires(ctx, rt)
	assert.NoError(t, err)

	// then actually work on the event
	task, err := queue.PopNextTask(rc, queue.BatchQueue)
	assert.NoError(t, err)
	assert.NotNil(t, task)

	typedTask, err := tasks.ReadTask(task.Type, task.Task)
	require.NoError(t, err)

	// work on that task
	err = typedTask.Perform(ctx, rt, models.OrgID(task.OrgID))
	assert.NoError(t, err)

	// should now have a flow start created
	assertdb.Query(t, db, `SELECT COUNT(*) from flows_flowstart WHERE flow_id = $1 AND start_type = 'T' AND status = 'P';`, testdata.IVRFlow.ID).Returns(1)
	assertdb.Query(t, db, `SELECT COUNT(*) from flows_flowstart_contacts WHERE contact_id = $1 AND flowstart_id = 1;`, testdata.Cathy.ID).Returns(1)
	assertdb.Query(t, db, `SELECT COUNT(*) from flows_flowstart_contacts WHERE contact_id = $1 AND flowstart_id = 1;`, testdata.George.ID).Returns(1)

	// event should be marked as fired
	assertdb.Query(t, db, `SELECT COUNT(*) from campaigns_eventfire WHERE event_id = $1 AND fired IS NOT NULL;`, testdata.RemindersEvent1.ID).Returns(2)

	// pop our next task, should be the start
	task, err = queue.PopNextTask(rc, queue.BatchQueue)
	assert.NoError(t, err)
	assert.NotNil(t, task)

	assert.Equal(t, task.Type, queue.StartIVRFlowBatch)
}

func assertFireTasks(t *testing.T, rp *redis.Pool, org *testdata.Org, expected [][]models.FireID) {
	allTasks := testsuite.CurrentOrgTasks(t, rp)
	actual := make([][]models.FireID, len(allTasks[org.ID]))

	for i, task := range allTasks[org.ID] {
		payload, err := jsonx.DecodeGeneric(task.Task)
		require.NoError(t, err)

		taskFireInts := payload.(map[string]interface{})["fire_ids"].([]interface{})
		taskFireIDs := make([]models.FireID, len(taskFireInts))
		for i := range taskFireInts {
			id, _ := strconv.Atoi(string(taskFireInts[i].(json.Number)))
			taskFireIDs[i] = models.FireID(int64(id))
		}
		actual[i] = taskFireIDs
	}

	assert.ElementsMatch(t, expected, actual)
}
