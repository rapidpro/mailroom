package campaigns

import (
	"testing"
	"time"

	_ "github.com/nyaruka/mailroom/hooks"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/queue"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/stretchr/testify/assert"
)

func TestCampaigns(t *testing.T) {
	testsuite.Reset()
	ctx := testsuite.CTX()
	rp := testsuite.RP()
	rc := testsuite.RC()
	defer rc.Close()

	// let's create a campaign event fire for one of our contacts (for now this is totally hacked, they aren't in the group and
	// their relative to date isn't relative, but this still tests execution)
	db := testsuite.DB()
	db.MustExec(`INSERT INTO campaigns_eventfire(scheduled, contact_id, event_id) VALUES (NOW(), $1, $3), (NOW(), $2, $3);`, models.CathyID, models.GeorgeID, models.RemindersEvent1ID)
	time.Sleep(10 * time.Millisecond)

	// schedule our campaign to be started
	err := fireCampaignEvents(ctx, db, rp, campaignsLock, "lock")
	assert.NoError(t, err)

	// then actually work on the event
	task, err := queue.PopNextTask(rc, queue.BatchQueue)
	assert.NoError(t, err)
	assert.NotNil(t, task)

	// work on that task
	err = fireEventFires(ctx, db, rp, task)
	assert.NoError(t, err)

	// should now have a flow run for that contact and flow
	testsuite.AssertQueryCount(t, db, `SELECT COUNT(*) from flows_flowrun WHERE contact_id = $1 AND flow_id = $2;`, []interface{}{models.CathyID, models.FavoritesFlowID}, 1)
	testsuite.AssertQueryCount(t, db, `SELECT COUNT(*) from flows_flowrun WHERE contact_id = $1 AND flow_id = $2;`, []interface{}{models.GeorgeID, models.FavoritesFlowID}, 1)
}

func TestIVRCampaigns(t *testing.T) {
	testsuite.Reset()
	ctx := testsuite.CTX()
	rp := testsuite.RP()
	rc := testsuite.RC()
	defer rc.Close()

	// let's create a campaign event fire for one of our contacts (for now this is totally hacked, they aren't in the group and
	// their relative to date isn't relative, but this still tests execution)
	db := testsuite.DB()
	db.MustExec(`UPDATE campaigns_campaignevent SET flow_id = $1 WHERE id = $2`, models.IVRFlowID, models.RemindersEvent1ID)
	db.MustExec(`INSERT INTO campaigns_eventfire(scheduled, contact_id, event_id) VALUES (NOW(), $1, $3), (NOW(), $2, $3);`, models.CathyID, models.GeorgeID, models.RemindersEvent1ID)
	time.Sleep(10 * time.Millisecond)

	// schedule our campaign to be started
	err := fireCampaignEvents(ctx, db, rp, campaignsLock, "lock")
	assert.NoError(t, err)

	// then actually work on the event
	task, err := queue.PopNextTask(rc, queue.BatchQueue)
	assert.NoError(t, err)
	assert.NotNil(t, task)

	// work on that task
	err = fireEventFires(ctx, db, rp, task)
	assert.NoError(t, err)

	// should now have a flow start created
	testsuite.AssertQueryCount(t, db, `SELECT COUNT(*) from flows_flowstart WHERE flow_id = $1 AND start_type = 'T' AND status = 'P';`, []interface{}{models.IVRFlowID}, 1)
	testsuite.AssertQueryCount(t, db, `SELECT COUNT(*) from flows_flowstart_contacts WHERE contact_id = $1 AND flowstart_id = 1;`, []interface{}{models.CathyID}, 1)
	testsuite.AssertQueryCount(t, db, `SELECT COUNT(*) from flows_flowstart_contacts WHERE contact_id = $1 AND flowstart_id = 1;`, []interface{}{models.GeorgeID}, 1)

	// event should be marked as fired
	testsuite.AssertQueryCount(t, db, `SELECT COUNT(*) from campaigns_eventfire WHERE event_id = $1 AND fired IS NOT NULL;`, []interface{}{models.RemindersEvent1ID}, 2)

	// pop our next task, should be the start
	task, err = queue.PopNextTask(rc, queue.BatchQueue)
	assert.NoError(t, err)
	assert.NotNil(t, task)

	assert.Equal(t, task.Type, queue.StartIVRFlowBatch)
}
