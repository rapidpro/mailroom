package campaigns_test

import (
	"encoding/json"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/nyaruka/gocommon/dbutil/assertdb"
	"github.com/nyaruka/gocommon/i18n"
	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/goflow/flows"
	_ "github.com/nyaruka/mailroom/core/handlers"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/queue"
	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/nyaruka/mailroom/core/tasks/campaigns"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueueEventFires(t *testing.T) {
	ctx, rt := testsuite.Runtime()
	rc := rt.RP.Get()
	defer rc.Close()

	defer testsuite.Reset(testsuite.ResetData | testsuite.ResetRedis)

	org2Campaign := testdata.InsertCampaign(rt, testdata.Org2, "Org 2", testdata.DoctorsGroup)
	org2CampaignEvent := testdata.InsertCampaignFlowEvent(rt, org2Campaign, testdata.Org2Favorites, testdata.AgeField, 1, "D")

	// try with zero fires
	cron := &campaigns.QueueEventsCron{}
	res, err := cron.Run(ctx, rt)
	assert.NoError(t, err)
	assert.Equal(t, map[string]any{"fires": 0, "dupes": 0, "tasks": 0}, res)

	assertFireTasks(t, rt, testdata.Org1, [][]models.FireID{})
	assertFireTasks(t, rt, testdata.Org2, [][]models.FireID{})

	// create event fires due now for 2 contacts and in the future for another contact
	fire1ID := testdata.InsertEventFire(rt, testdata.Cathy, testdata.RemindersEvent1, time.Now().Add(-time.Minute))
	fire2ID := testdata.InsertEventFire(rt, testdata.George, testdata.RemindersEvent1, time.Now().Add(-time.Minute))
	fire3ID := testdata.InsertEventFire(rt, testdata.Org2Contact, org2CampaignEvent, time.Now().Add(-time.Minute))
	fire4ID := testdata.InsertEventFire(rt, testdata.Alexandria, testdata.RemindersEvent2, time.Now().Add(-time.Minute))
	testdata.InsertEventFire(rt, testdata.Alexandria, testdata.RemindersEvent1, time.Now().Add(time.Hour*24)) // in future

	// schedule our campaign to be started
	res, err = cron.Run(ctx, rt)
	assert.NoError(t, err)
	assert.Equal(t, map[string]any{"fires": 4, "dupes": 0, "tasks": 3}, res)

	assertFireTasks(t, rt, testdata.Org1, [][]models.FireID{{fire1ID, fire2ID}, {fire4ID}})
	assertFireTasks(t, rt, testdata.Org2, [][]models.FireID{{fire3ID}})

	// running again won't double add those fires
	res, err = cron.Run(ctx, rt)
	assert.NoError(t, err)
	assert.Equal(t, map[string]any{"fires": 4, "dupes": 4, "tasks": 0}, res)

	assertFireTasks(t, rt, testdata.Org1, [][]models.FireID{{fire1ID, fire2ID}, {fire4ID}})
	assertFireTasks(t, rt, testdata.Org2, [][]models.FireID{{fire3ID}})

	// clear queued tasks
	rc.Do("DEL", "batch:active")
	rc.Do("DEL", "batch:1")

	// add 110 scheduled event fires to test batch limits
	for i := 0; i < 110; i++ {
		contact := testdata.InsertContact(rt, testdata.Org1, flows.ContactUUID(uuids.New()), fmt.Sprintf("Jim %d", i), i18n.NilLanguage, models.ContactStatusActive)
		testdata.InsertEventFire(rt, contact, testdata.RemindersEvent1, time.Now().Add(-time.Minute))
	}

	res, err = cron.Run(ctx, rt)
	assert.NoError(t, err)
	assert.Equal(t, map[string]any{"fires": 114, "dupes": 4, "tasks": 2}, res)

	queuedTasks := testsuite.CurrentTasks(t, rt)
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
func TestQueueAndFireEvent(t *testing.T) {
	ctx, rt := testsuite.Runtime()
	rc := rt.RP.Get()
	defer rc.Close()

	defer testsuite.Reset(testsuite.ResetData | testsuite.ResetRedis)

	// create due fires for Cathy and George
	f1ID := testdata.InsertEventFire(rt, testdata.Cathy, testdata.RemindersEvent1, time.Now().Add(-time.Minute))
	f2ID := testdata.InsertEventFire(rt, testdata.George, testdata.RemindersEvent1, time.Now().Add(-time.Minute))

	// queue the event task
	cron := &campaigns.QueueEventsCron{}
	_, err := cron.Run(ctx, rt)
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
	assertdb.Query(t, rt.DB, `SELECT COUNT(*) FROM flows_flowrun WHERE contact_id = $1 AND flow_id = $2 AND status = 'W'`, testdata.Cathy.ID, testdata.Favorites.ID).Returns(1)
	assertdb.Query(t, rt.DB, `SELECT COUNT(*) FROM flows_flowrun WHERE contact_id = $1 AND flow_id = $2 AND status = 'W'`, testdata.George.ID, testdata.Favorites.ID).Returns(1)

	// the event fires should be marked as fired
	assertdb.Query(t, rt.DB, `SELECT fired IS NOT NULL AS fired, fired_result FROM campaigns_eventfire WHERE id = $1`, f1ID).Columns(map[string]any{"fired": true, "fired_result": "F"})
	assertdb.Query(t, rt.DB, `SELECT fired IS NOT NULL AS fired, fired_result FROM campaigns_eventfire WHERE id = $1`, f2ID).Columns(map[string]any{"fired": true, "fired_result": "F"})

	// create due fires for George and Bob for a different event that skips
	f3ID := testdata.InsertEventFire(rt, testdata.George, testdata.RemindersEvent3, time.Now().Add(-time.Minute))
	f4ID := testdata.InsertEventFire(rt, testdata.Bob, testdata.RemindersEvent3, time.Now().Add(-time.Minute))

	// queue the event task
	_, err = cron.Run(ctx, rt)
	assert.NoError(t, err)

	// then actually work on the event
	task, err = queue.PopNextTask(rc, queue.BatchQueue)
	assert.NoError(t, err)
	assert.NotNil(t, task)

	typedTask, err = tasks.ReadTask(task.Type, task.Task)
	require.NoError(t, err)

	// work on that task
	err = typedTask.Perform(ctx, rt, models.OrgID(task.OrgID))
	assert.NoError(t, err)

	assertdb.Query(t, rt.DB, `SELECT COUNT(*) FROM flows_flowrun WHERE contact_id = $1 AND flow_id = $2 AND status = 'W'`, testdata.George.ID, testdata.Favorites.ID).Returns(1)
	assertdb.Query(t, rt.DB, `SELECT COUNT(*) FROM flows_flowrun WHERE contact_id = $1 AND flow_id = $2 AND status = 'W'`, testdata.Bob.ID, testdata.PickANumber.ID).Returns(1)

	// the event fires should be marked as fired
	assertdb.Query(t, rt.DB, `SELECT fired IS NOT NULL AS fired, fired_result FROM campaigns_eventfire WHERE id = $1`, f3ID).Columns(map[string]any{"fired": true, "fired_result": "S"})
	assertdb.Query(t, rt.DB, `SELECT fired IS NOT NULL AS fired, fired_result FROM campaigns_eventfire WHERE id = $1`, f4ID).Columns(map[string]any{"fired": true, "fired_result": "F"})
}

func TestIVRCampaigns(t *testing.T) {
	ctx, rt := testsuite.Runtime()
	rc := rt.RP.Get()
	defer rc.Close()

	defer testsuite.Reset(testsuite.ResetAll)

	// turn a campaign event into an IVR flow event
	rt.DB.MustExec(`UPDATE campaigns_campaignevent SET flow_id = $1 WHERE id = $2`, testdata.IVRFlow.ID, testdata.RemindersEvent1.ID)

	testdata.InsertEventFire(rt, testdata.Cathy, testdata.RemindersEvent1, time.Now().Add(-time.Minute))
	testdata.InsertEventFire(rt, testdata.George, testdata.RemindersEvent1, time.Now().Add(-time.Minute))

	// schedule our campaign to be started
	cron := &campaigns.QueueEventsCron{}
	_, err := cron.Run(ctx, rt)
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
	assertdb.Query(t, rt.DB, `SELECT COUNT(*) from flows_flowstart WHERE flow_id = $1 AND start_type = 'T' AND status = 'P';`, testdata.IVRFlow.ID).Returns(1)
	assertdb.Query(t, rt.DB, `SELECT COUNT(*) from flows_flowstart_contacts WHERE contact_id = $1 AND flowstart_id = 1;`, testdata.Cathy.ID).Returns(1)
	assertdb.Query(t, rt.DB, `SELECT COUNT(*) from flows_flowstart_contacts WHERE contact_id = $1 AND flowstart_id = 1;`, testdata.George.ID).Returns(1)

	// event should be marked as fired
	assertdb.Query(t, rt.DB, `SELECT COUNT(*) from campaigns_eventfire WHERE event_id = $1 AND fired IS NOT NULL;`, testdata.RemindersEvent1.ID).Returns(2)

	// pop our next task, should be the start
	task, err = queue.PopNextTask(rc, queue.BatchQueue)
	assert.NoError(t, err)
	assert.NotNil(t, task)

	assert.Equal(t, "start_ivr_flow_batch", task.Type)
}

func assertFireTasks(t *testing.T, rt *runtime.Runtime, org *testdata.Org, expected [][]models.FireID) {
	allTasks := testsuite.CurrentTasks(t, rt)
	actual := make([][]models.FireID, len(allTasks[org.ID]))

	for i, task := range allTasks[org.ID] {
		payload, err := jsonx.DecodeGeneric(task.Task)
		require.NoError(t, err)

		taskFireInts := payload.(map[string]any)["fire_ids"].([]any)
		taskFireIDs := make([]models.FireID, len(taskFireInts))
		for i := range taskFireInts {
			id, _ := strconv.Atoi(string(taskFireInts[i].(json.Number)))
			taskFireIDs[i] = models.FireID(int64(id))
		}
		actual[i] = taskFireIDs
	}

	assert.ElementsMatch(t, expected, actual)
}
