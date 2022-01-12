package campaigns_test

import (
	"encoding/json"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/gocommon/dbutil/assertdb"
	"github.com/nyaruka/gocommon/jsonx"
	_ "github.com/nyaruka/mailroom/core/handlers"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/queue"
	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/nyaruka/mailroom/core/tasks/campaigns"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/nyaruka/redisx/assertredis"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFireCampaignEvents(t *testing.T) {
	ctx, rt, db, rp := testsuite.Get()
	rc := rp.Get()
	defer rc.Close()

	defer testsuite.Reset(testsuite.ResetData | testsuite.ResetRedis)

	// try with zero fires
	err := campaigns.FireCampaignEvents(ctx, rt)
	assert.NoError(t, err)

	assertredis.ZCard(t, rp, "batch:active", 0)
	assertredis.ZCard(t, rp, "batch:1", 0)

	// create event fires due now for 2 contacts and in the future for another contact
	cathyFire1ID := scheduleFire(rt.DB, testdata.Cathy, testdata.RemindersEvent1, time.Now())
	georgeFire1ID := scheduleFire(rt.DB, testdata.George, testdata.RemindersEvent1, time.Now())
	scheduleFire(rt.DB, testdata.Alexandria, testdata.RemindersEvent1, time.Now().Add(time.Hour*24))
	time.Sleep(10 * time.Millisecond)

	// schedule our campaign to be started
	err = campaigns.FireCampaignEvents(ctx, rt)
	assert.NoError(t, err)

	assertredis.ZCard(t, rp, "batch:active", 1)
	assertredis.ZRange(t, rp, "batch:active", 0, -1, []string{"1"})

	assertFireTasks(t, rc, testdata.Org1, [][]models.FireID{{cathyFire1ID, georgeFire1ID}})

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

	// let's create a campaign event fire for one of our contacts (for now this is totally hacked, they aren't in the group and
	// their relative to date isn't relative, but this still tests execution)
	rt.DB.MustExec(`UPDATE campaigns_campaignevent SET flow_id = $1 WHERE id = $2`, testdata.IVRFlow.ID, testdata.RemindersEvent1.ID)
	rt.DB.MustExec(`INSERT INTO campaigns_eventfire(scheduled, contact_id, event_id) VALUES (NOW(), $1, $3), (NOW(), $2, $3);`, testdata.Cathy.ID, testdata.George.ID, testdata.RemindersEvent1.ID)
	time.Sleep(10 * time.Millisecond)

	// schedule our campaign to be started
	err := campaigns.FireCampaignEvents(ctx, rt)
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

func assertFireTasks(t *testing.T, rc redis.Conn, org *testdata.Org, expected [][]models.FireID) {
	var actual [][]models.FireID
	tasks, err := redis.Strings(rc.Do("ZRANGE", fmt.Sprintf("batch:%d", org.ID), 0, -1))
	require.NoError(t, err)

	for _, taskJSON := range tasks {
		fmt.Println(string(taskJSON))

		taskAsMap, err := jsonx.DecodeGeneric([]byte(taskJSON))
		require.NoError(t, err)

		taskFireInts := taskAsMap.(map[string]interface{})["task"].(map[string]interface{})["fire_ids"].([]interface{})
		taskFireIDs := make([]models.FireID, len(taskFireInts))
		for i := range taskFireInts {
			id, _ := strconv.Atoi(string(taskFireInts[i].(json.Number)))
			taskFireIDs[i] = models.FireID(int64(id))
		}
		actual = append(actual, taskFireIDs)
	}

	assert.ElementsMatch(t, expected, actual)
}

func scheduleFire(db *sqlx.DB, contact *testdata.Contact, event *testdata.CampaignEvent, when time.Time) models.FireID {
	var id models.FireID
	db.Get(&id, `INSERT INTO campaigns_eventfire(contact_id, event_id, scheduled) VALUES ($1, $2, $3) RETURNING id;`, contact.ID, event.ID, when)
	return id
}
