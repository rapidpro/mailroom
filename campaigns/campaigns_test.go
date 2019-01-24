package campaigns

import (
	"os"
	"testing"
	"time"

	"github.com/nyaruka/mailroom"

	_ "github.com/nyaruka/mailroom/hooks"
	"github.com/nyaruka/mailroom/marker"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/queue"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	testsuite.Reset()
	os.Exit(m.Run())
}

func TestCampaigns(t *testing.T) {
	ctx := testsuite.CTX()
	rp := testsuite.RP()
	rc := testsuite.RC()
	defer rc.Close()

	err := marker.ClearTasks(rc, campaignsLock)
	assert.NoError(t, err)

	// let's create a campaign event fire for one of our contacts (for now this is totally hacked, they aren't in the group and
	// their relative to date isn't relative, but this still tests execution)
	db := testsuite.DB()
	db.MustExec(`UPDATE flows_flow SET flow_server_enabled=TRUE WHERE id = $1;`, models.FavoritesFlowID)
	db.MustExec(`INSERT INTO campaigns_eventfire(scheduled, contact_id, event_id) VALUES (NOW(), $1, $3), (NOW(), $2, $3);`, models.CathyID, models.GeorgeID, models.RemindersEvent1ID)
	time.Sleep(10 * time.Millisecond)

	// schedule our campaign to be started
	err = fireCampaignEvents(ctx, db, rp, campaignsLock, "lock")
	assert.NoError(t, err)

	// then actually work on the event
	task, err := queue.PopNextTask(rc, mailroom.BatchQueue)
	assert.NoError(t, err)
	assert.NotNil(t, task)

	// work on that task
	err = fireEventFires(ctx, db, rp, task)
	assert.NoError(t, err)

	// should now have a flow run for that contact and flow
	testsuite.AssertQueryCount(t, db, `SELECT COUNT(*) from flows_flowrun WHERE contact_id = $1 AND flow_id = $2;`, []interface{}{models.CathyID, models.FavoritesFlowID}, 1)
	testsuite.AssertQueryCount(t, db, `SELECT COUNT(*) from flows_flowrun WHERE contact_id = $1 AND flow_id = $2;`, []interface{}{models.GeorgeID, models.FavoritesFlowID}, 1)
}
