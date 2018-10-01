package campaigns

import (
	"os"
	"testing"
	"time"

	"github.com/nyaruka/mailroom"

	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/mailroom/marker"
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
	db.MustExec(`UPDATE flows_flow SET flow_server_enabled=TRUE WHERE id = 31;`)
	db.MustExec(`INSERT INTO campaigns_eventfire(scheduled, contact_id, event_id) VALUES (NOW(), 42, 2), (NOW(), 43, 2);`)
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
	assertCount(t, db, `SELECT COUNT(*) from flows_flowrun WHERE contact_id = 42 AND flow_id = 31;`, 1)
	assertCount(t, db, `SELECT COUNT(*) from flows_flowrun WHERE contact_id = 43 AND flow_id = 31;`, 1)
}

func assertCount(t *testing.T, db *sqlx.DB, query string, count int) {
	var c int
	err := db.Get(&c, query)
	assert.NoError(t, err)
	assert.Equal(t, count, c)
}
