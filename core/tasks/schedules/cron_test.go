package schedules

import (
	"testing"

	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/queue"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"

	"github.com/stretchr/testify/assert"
)

func TestCheckSchedules(t *testing.T) {
	ctx, rt, db, rp := testsuite.Get()
	rc := rp.Get()
	defer rc.Close()

	defer testsuite.Reset(testsuite.ResetAll)

	// add a schedule and tie a broadcast to it
	var s1 models.ScheduleID
	err := db.Get(
		&s1,
		`INSERT INTO schedules_schedule(is_active, repeat_period, created_on, modified_on, next_fire, created_by_id, modified_by_id, org_id)
			VALUES(TRUE, 'O', NOW(), NOW(), NOW()- INTERVAL '1 DAY', 1, 1, $1) RETURNING id`,
		testdata.Org1.ID,
	)
	assert.NoError(t, err)

	b1 := testdata.InsertBroadcast(db, testdata.Org1, "eng", map[envs.Language]string{"eng": "Test message", "fra": "Un Message"}, s1,
		[]*testdata.Contact{testdata.Cathy, testdata.George}, []*testdata.Group{testdata.DoctorsGroup},
	)

	// add a URN
	db.MustExec(`INSERT INTO msgs_broadcast_urns(broadcast_id, contacturn_id) VALUES($1, $2)`, b1, testdata.Cathy.URNID)

	// add another and tie a trigger to it
	var s2 models.ScheduleID
	err = db.Get(
		&s2,
		`INSERT INTO schedules_schedule(is_active, repeat_period, created_on, modified_on, next_fire, created_by_id, modified_by_id, org_id)
			VALUES(TRUE, 'O', NOW(), NOW(), NOW()- INTERVAL '2 DAY', 1, 1, $1) RETURNING id`,
		testdata.Org1.ID,
	)
	assert.NoError(t, err)
	var t1 models.TriggerID
	err = db.Get(
		&t1,
		`INSERT INTO triggers_trigger(is_active, created_on, modified_on, is_archived, trigger_type, created_by_id, modified_by_id, org_id, flow_id, schedule_id)
			VALUES(TRUE, NOW(), NOW(), FALSE, 'S', 1, 1, $1, $2, $3) RETURNING id`,
		testdata.Org1.ID, testdata.Favorites.ID, s2,
	)
	assert.NoError(t, err)

	// add a few contacts to the trigger
	db.MustExec(`INSERT INTO triggers_trigger_contacts(trigger_id, contact_id) VALUES($1, $2),($1, $3)`, t1, testdata.Cathy.ID, testdata.George.ID)

	// and a group
	db.MustExec(`INSERT INTO triggers_trigger_groups(trigger_id, contactgroup_id) VALUES($1, $2)`, t1, testdata.DoctorsGroup.ID)

	var s3 models.ScheduleID
	err = db.Get(
		&s3,
		`INSERT INTO schedules_schedule(is_active, repeat_period, created_on, modified_on, next_fire, created_by_id, modified_by_id, org_id)
			VALUES(TRUE, 'O', NOW(), NOW(), NOW()- INTERVAL '3 DAY', 1, 1, $1) RETURNING id`,
		testdata.Org1.ID,
	)
	assert.NoError(t, err)

	// run our task
	err = checkSchedules(ctx, rt)
	assert.NoError(t, err)

	// should have one flow start added to our DB ready to go
	testsuite.AssertQuery(t, db, `SELECT count(*) FROM flows_flowstart WHERE flow_id = $1 AND start_type = 'T' AND status = 'P'`, testdata.Favorites.ID).Returns(1)

	// with the right count of groups and contacts
	testsuite.AssertQuery(t, db, `SELECT count(*) from flows_flowstart_contacts WHERE flowstart_id = 1`).Returns(2)
	testsuite.AssertQuery(t, db, `SELECT count(*) from flows_flowstart_groups WHERE flowstart_id = 1`).Returns(1)

	// and one broadcast as well
	testsuite.AssertQuery(t, db, `SELECT count(*) FROM msgs_broadcast WHERE org_id = $1 AND parent_id = $2 
		AND text = hstore(ARRAY['eng','Test message', 'fra', 'Un Message']) AND status = 'Q' AND base_language = 'eng'`, testdata.Org1.ID, b1).Returns(1)

	// with the right count of groups, contacts, urns
	testsuite.AssertQuery(t, db, `SELECT count(*) from msgs_broadcast_urns WHERE broadcast_id = 2`).Returns(1)
	testsuite.AssertQuery(t, db, `SELECT count(*) from msgs_broadcast_contacts WHERE broadcast_id = 2`).Returns(2)
	testsuite.AssertQuery(t, db, `SELECT count(*) from msgs_broadcast_groups WHERE broadcast_id = 2`).Returns(1)

	// we shouldn't have any pending schedules since there were all one time fires, but all should have last fire
	testsuite.AssertQuery(t, db, `SELECT count(*) FROM schedules_schedule WHERE next_fire IS NULL and last_fire < NOW();`).Returns(3)

	// check the tasks created
	task, err := queue.PopNextTask(rc, queue.BatchQueue)

	// first should be the flow start
	assert.NoError(t, err)
	assert.NotNil(t, task)
	assert.Equal(t, queue.StartFlow, task.Type)

	// then the broadacast
	task, err = queue.PopNextTask(rc, queue.BatchQueue)
	assert.NoError(t, err)
	assert.NotNil(t, task)
	assert.Equal(t, queue.SendBroadcast, task.Type)

	// nothing more
	task, err = queue.PopNextTask(rc, queue.BatchQueue)
	assert.NoError(t, err)
	assert.Nil(t, task)
}
