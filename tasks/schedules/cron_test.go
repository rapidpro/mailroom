package schedules

import (
	"testing"

	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/queue"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/stretchr/testify/assert"
)

func TestCheckSchedules(t *testing.T) {
	testsuite.Reset()
	ctx := testsuite.CTX()
	rp := testsuite.RP()

	rc := rp.Get()
	defer rc.Close()

	// add a schedule and tie a broadcast to it
	db := testsuite.DB()
	var s1 models.ScheduleID
	err := db.Get(
		&s1,
		`INSERT INTO schedules_schedule(is_active, repeat_period, created_on, modified_on, next_fire, created_by_id, modified_by_id, org_id)
			VALUES(TRUE, 'O', NOW(), NOW(), NOW()- INTERVAL '1 DAY', 1, 1, $1) RETURNING id`,
		models.Org1,
	)
	assert.NoError(t, err)
	var b1 models.BroadcastID
	err = db.Get(
		&b1,
		`INSERT INTO msgs_broadcast(status, text, base_language, is_active, created_on, modified_on, send_all, created_by_id, modified_by_id, org_id, schedule_id)
			VALUES('P', hstore(ARRAY['eng','Test message', 'fra', 'Un Message']), 'eng', TRUE, NOW(), NOW(), TRUE, 1, 1, $1, $2) RETURNING id`,
		models.Org1, s1,
	)
	assert.NoError(t, err)

	// add a few contacts to the broadcast
	db.MustExec(`INSERT INTO msgs_broadcast_contacts(broadcast_id, contact_id) VALUES($1, $2),($1, $3)`, b1, models.CathyID, models.GeorgeID)

	// and a group
	db.MustExec(`INSERT INTO msgs_broadcast_groups(broadcast_id, contactgroup_id) VALUES($1, $2)`, b1, models.DoctorsGroupID)

	// and a URN
	db.MustExec(`INSERT INTO msgs_broadcast_urns(broadcast_id, contacturn_id) VALUES($1, $2)`, b1, models.CathyURNID)

	// add another and tie a trigger to it
	var s2 models.ScheduleID
	err = db.Get(
		&s2,
		`INSERT INTO schedules_schedule(is_active, repeat_period, created_on, modified_on, next_fire, created_by_id, modified_by_id, org_id)
			VALUES(TRUE, 'O', NOW(), NOW(), NOW()- INTERVAL '2 DAY', 1, 1, $1) RETURNING id`,
		models.Org1,
	)
	assert.NoError(t, err)
	var t1 models.TriggerID
	err = db.Get(
		&t1,
		`INSERT INTO triggers_trigger(is_active, created_on, modified_on, is_archived, trigger_type, created_by_id, modified_by_id, org_id, flow_id, schedule_id)
			VALUES(TRUE, NOW(), NOW(), FALSE, 'S', 1, 1, $1, $2, $3) RETURNING id`,
		models.Org1, models.FavoritesFlowID, s2,
	)
	assert.NoError(t, err)

	// add a few contacts to the trigger
	db.MustExec(`INSERT INTO triggers_trigger_contacts(trigger_id, contact_id) VALUES($1, $2),($1, $3)`, t1, models.CathyID, models.GeorgeID)

	// and a group
	db.MustExec(`INSERT INTO triggers_trigger_groups(trigger_id, contactgroup_id) VALUES($1, $2)`, t1, models.DoctorsGroupID)

	var s3 models.ScheduleID
	err = db.Get(
		&s3,
		`INSERT INTO schedules_schedule(is_active, repeat_period, created_on, modified_on, next_fire, created_by_id, modified_by_id, org_id)
			VALUES(TRUE, 'O', NOW(), NOW(), NOW()- INTERVAL '3 DAY', 1, 1, $1) RETURNING id`,
		models.Org1,
	)
	assert.NoError(t, err)

	// run our task
	err = checkSchedules(ctx, db, rp, "lock", "lock")
	assert.NoError(t, err)

	// should have one flow start added to our DB ready to go
	testsuite.AssertQueryCount(t, db,
		`SELECT count(*) FROM flows_flowstart WHERE flow_id = $1 AND start_type = 'T' AND status = 'P';`,
		[]interface{}{models.FavoritesFlowID}, 1)

	// with the right count of groups and contacts
	testsuite.AssertQueryCount(t, db, `SELECT count(*) from flows_flowstart_contacts WHERE flowstart_id = 1`, nil, 2)
	testsuite.AssertQueryCount(t, db, `SELECT count(*) from flows_flowstart_groups WHERE flowstart_id = 1`, nil, 1)

	// and one broadcast as well
	testsuite.AssertQueryCount(t, db,
		`SELECT count(*) FROM msgs_broadcast WHERE org_id = $1 AND parent_id = $2 AND text = hstore(ARRAY['eng','Test message', 'fra', 'Un Message'])
		AND status = 'Q' AND base_language = 'eng';`,
		[]interface{}{models.Org1, b1}, 1)

	// with the right count of groups, contacts, urns
	testsuite.AssertQueryCount(t, db, `SELECT count(*) from msgs_broadcast_urns WHERE broadcast_id = 2`, nil, 1)
	testsuite.AssertQueryCount(t, db, `SELECT count(*) from msgs_broadcast_contacts WHERE broadcast_id = 2`, nil, 2)
	testsuite.AssertQueryCount(t, db, `SELECT count(*) from msgs_broadcast_groups WHERE broadcast_id = 2`, nil, 1)

	// we shouldn't have any pending schedules since there were all one time fires, but all should have last fire
	testsuite.AssertQueryCount(t, db,
		`SELECT count(*) FROM schedules_schedule WHERE next_fire IS NULL and last_fire < NOW();`,
		nil, 3)

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
