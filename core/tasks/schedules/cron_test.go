package schedules

import (
	"testing"
	"time"

	"github.com/nyaruka/gocommon/dbutil/assertdb"
	"github.com/nyaruka/gocommon/i18n"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/stretchr/testify/assert"
)

func TestCheckSchedules(t *testing.T) {
	ctx, rt := testsuite.Runtime()
	rc := rt.RP.Get()
	defer rc.Close()

	defer testsuite.Reset(testsuite.ResetData | testsuite.ResetRedis)

	// add a one-time schedule and tie a broadcast to it
	s1 := testdata.InsertSchedule(rt, testdata.Org1, models.RepeatPeriodNever, time.Now().Add(-2*time.Hour))
	b1 := testdata.InsertBroadcast(rt, testdata.Org1, "eng", map[i18n.Language]string{"eng": "Hi", "spa": "Hola"}, nil, s1, []*testdata.Contact{testdata.Cathy, testdata.George}, nil)

	// add a repeating schedule and tie another broadcast to it
	s2 := testdata.InsertSchedule(rt, testdata.Org1, models.RepeatPeriodDaily, time.Now().Add(-time.Hour))
	b2 := testdata.InsertBroadcast(rt, testdata.Org1, "eng", map[i18n.Language]string{"eng": "Bye", "spa": "Chau"}, nil, s2, nil, []*testdata.Group{testdata.DoctorsGroup})

	// add a one-time schedule and tie a trigger to it
	s3 := testdata.InsertSchedule(rt, testdata.Org1, models.RepeatPeriodNever, time.Now().Add(-2*time.Hour))
	t1 := testdata.InsertScheduledTrigger(rt, testdata.Org1, testdata.Favorites, s3, nil, nil, []*testdata.Contact{testdata.Cathy, testdata.George})

	// add a repeating schedule and tie another trigger to it
	s4 := testdata.InsertSchedule(rt, testdata.Org1, models.RepeatPeriodDaily, time.Now().Add(-time.Hour))
	testdata.InsertScheduledTrigger(rt, testdata.Org1, testdata.Favorites, s4, []*testdata.Group{testdata.DoctorsGroup}, nil, nil)

	// add a repeating orphaned schedule
	testdata.InsertSchedule(rt, testdata.Org1, models.RepeatPeriodDaily, time.Now().Add(-time.Hour))

	// run our task
	cron := &schedulesCron{}
	res, err := cron.Run(ctx, rt)
	assert.NoError(t, err)
	assert.Equal(t, map[string]any{"broadcasts": 2, "triggers": 2, "noops": 1}, res)

	// should have 2 flow starts added to our DB ready to go
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM flows_flowstart WHERE flow_id = $1 AND start_type = 'T' AND status = 'P'`, testdata.Favorites.ID).Returns(2)

	// with the right counts of groups and contacts
	assertdb.Query(t, rt.DB, `SELECT count(*) from flows_flowstart_contacts WHERE flowstart_id = 1`).Returns(2)
	assertdb.Query(t, rt.DB, `SELECT count(*) from flows_flowstart_groups WHERE flowstart_id = 2`).Returns(1)

	// and two child broadcasts as well
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM msgs_broadcast WHERE org_id = $1 
		AND parent_id = $2 
		AND translations -> 'eng' ->> 'text' = 'Hi'
		AND translations -> 'spa' ->> 'text' = 'Hola'
		AND status = 'Q' 
		AND base_language = 'eng'`, testdata.Org1.ID, b1).Returns(1)

	assertdb.Query(t, rt.DB, `SELECT count(*) FROM msgs_broadcast WHERE org_id = $1 
		AND parent_id = $2 
		AND translations -> 'eng' ->> 'text' = 'Bye'
		AND translations -> 'spa' ->> 'text' = 'Chau'
		AND status = 'Q' 
		AND base_language = 'eng'`, testdata.Org1.ID, b2).Returns(1)

	// with the right count of contacts and groups
	assertdb.Query(t, rt.DB, `SELECT count(*) from msgs_broadcast_contacts WHERE broadcast_id = 3`).Returns(2)
	assertdb.Query(t, rt.DB, `SELECT count(*) from msgs_broadcast_groups WHERE broadcast_id = 4`).Returns(1)

	// the one-off schedules should de deleted and their broadcast and trigger deactivated
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM schedules_schedule WHERE id = $1`, s1).Returns(0)
	assertdb.Query(t, rt.DB, `SELECT is_active FROM msgs_broadcast WHERE id = $1`, b1).Returns(false)
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM schedules_schedule WHERE id = $1`, s3).Returns(0)
	assertdb.Query(t, rt.DB, `SELECT is_active FROM triggers_trigger WHERE id = $1`, t1).Returns(false)

	// the repeating schedules should have next_fire and last_fire updated
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM schedules_schedule WHERE id = $1 AND next_fire > NOW() AND last_fire < NOW()`, s2).Returns(1)
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM schedules_schedule WHERE id = $1 AND next_fire > NOW() AND last_fire < NOW()`, s4).Returns(1)

	// check the tasks created
	testsuite.AssertBatchTasks(t, testdata.Org1.ID, map[string]int{"start_flow": 2, "send_broadcast": 2})
}
