package ivr_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/nyaruka/gocommon/dbutil/assertdb"
	"github.com/nyaruka/mailroom/core/ivr"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/queue"
	ivrtasks "github.com/nyaruka/mailroom/core/tasks/ivr"
	"github.com/nyaruka/mailroom/core/tasks/starts"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/stretchr/testify/assert"
)

func TestRetries(t *testing.T) {
	ctx, rt, db, rp := testsuite.Get()
	rc := rp.Get()
	defer rc.Close()

	defer testsuite.Reset(testsuite.ResetAll)

	// register our mock client
	ivr.RegisterServiceType(models.ChannelType("ZZ"), NewMockProvider)

	// update our twilio channel to be of type 'ZZ' and set max_concurrent_events to 1
	db.MustExec(`UPDATE channels_channel SET channel_type = 'ZZ', config = '{"max_concurrent_events": 1}' WHERE id = $1`, testdata.TwilioChannel.ID)

	// create a flow start for cathy
	start := models.NewFlowStart(testdata.Org1.ID, models.StartTypeTrigger, models.FlowTypeVoice, testdata.IVRFlow.ID, models.DoRestartParticipants, models.DoIncludeActive).
		WithContactIDs([]models.ContactID{testdata.Cathy.ID})

	// call our master starter
	err := starts.CreateFlowBatches(ctx, rt, start)
	assert.NoError(t, err)

	// should have one task in our ivr queue
	task, err := queue.PopNextTask(rc, queue.HandlerQueue)
	assert.NoError(t, err)
	batch := &models.FlowStartBatch{}
	err = json.Unmarshal(task.Task, batch)
	assert.NoError(t, err)

	client.callError = nil
	client.callID = ivr.CallID("call1")
	err = ivrtasks.HandleFlowStartBatch(ctx, rt, batch)
	assert.NoError(t, err)
	assertdb.Query(t, db, `SELECT COUNT(*) FROM channels_channelconnection WHERE contact_id = $1 AND status = $2 AND external_id = $3`,
		testdata.Cathy.ID, models.ConnectionStatusWired, "call1").Returns(1)

	// change our call to be errored instead of wired
	db.MustExec(`UPDATE channels_channelconnection SET status = 'E', next_attempt = NOW() WHERE external_id = 'call1';`)

	// fire our retries
	err = ivrtasks.RetryCalls(ctx, rt)
	assert.NoError(t, err)

	// should now be in wired state
	assertdb.Query(t, db, `SELECT COUNT(*) FROM channels_channelconnection WHERE contact_id = $1 AND status = $2 AND external_id = $3`,
		testdata.Cathy.ID, models.ConnectionStatusWired, "call1").Returns(1)

	// back to retry and make the channel inactive
	db.MustExec(`UPDATE channels_channelconnection SET status = 'E', next_attempt = NOW() WHERE external_id = 'call1';`)
	db.MustExec(`UPDATE channels_channel SET is_active = FALSE WHERE id = $1`, testdata.TwilioChannel.ID)

	models.FlushCache()
	err = ivrtasks.RetryCalls(ctx, rt)
	assert.NoError(t, err)

	// this time should be failed
	assertdb.Query(t, db, `SELECT COUNT(*) FROM channels_channelconnection WHERE contact_id = $1 AND status = $2 AND external_id = $3`,
		testdata.Cathy.ID, models.ConnectionStatusFailed, "call1").Returns(1)
}

func TestExpirations(t *testing.T) {
	ctx, rt, db, rp := testsuite.Get()
	rc := rp.Get()
	defer rc.Close()

	defer testsuite.Reset(testsuite.ResetData | testsuite.ResetRedis)

	// create voice session for Cathy
	conn1ID := testdata.InsertConnection(db, testdata.Org1, testdata.TwilioChannel, testdata.Cathy)
	s1ID := testdata.InsertWaitingSession(db, testdata.Org1, testdata.Cathy, models.FlowTypeVoice, testdata.IVRFlow, conn1ID, time.Now(), time.Now(), false, nil)
	r1ID := testdata.InsertFlowRun(db, testdata.Org1, s1ID, testdata.Cathy, testdata.Favorites, models.RunStatusWaiting, "", nil)

	// create voice session for Bob with expiration in future
	conn2ID := testdata.InsertConnection(db, testdata.Org1, testdata.TwilioChannel, testdata.Bob)
	s2ID := testdata.InsertWaitingSession(db, testdata.Org1, testdata.Bob, models.FlowTypeMessaging, testdata.IVRFlow, conn2ID, time.Now(), time.Now().Add(time.Hour), false, nil)
	r2ID := testdata.InsertFlowRun(db, testdata.Org1, s2ID, testdata.Bob, testdata.IVRFlow, models.RunStatusWaiting, "", nil)

	// create a messaging session for Alexandria
	s3ID := testdata.InsertWaitingSession(db, testdata.Org1, testdata.Alexandria, models.FlowTypeMessaging, testdata.Favorites, models.NilConnectionID, time.Now(), time.Now(), false, nil)
	r3ID := testdata.InsertFlowRun(db, testdata.Org1, s3ID, testdata.Alexandria, testdata.Favorites, models.RunStatusWaiting, "", nil)

	time.Sleep(5 * time.Millisecond)

	// expire our sessions...
	err := ivrtasks.ExpireVoiceSessions(ctx, rt)
	assert.NoError(t, err)

	// Cathy's session should be expired along with its runs
	assertdb.Query(t, db, `SELECT status FROM flows_flowsession WHERE id = $1;`, s1ID).Columns(map[string]interface{}{"status": "X"})
	assertdb.Query(t, db, `SELECT status FROM flows_flowrun WHERE id = $1;`, r1ID).Columns(map[string]interface{}{"status": "X"})

	// Bob's session and run should be unchanged
	assertdb.Query(t, db, `SELECT status FROM flows_flowsession WHERE id = $1;`, s2ID).Columns(map[string]interface{}{"status": "W"})
	assertdb.Query(t, db, `SELECT status FROM flows_flowrun WHERE id = $1;`, r2ID).Columns(map[string]interface{}{"status": "W"})

	// Alexandria's session and run should be unchanged because message expirations are handled separately
	assertdb.Query(t, db, `SELECT status FROM flows_flowsession WHERE id = $1;`, s3ID).Columns(map[string]interface{}{"status": "W"})
	assertdb.Query(t, db, `SELECT status FROM flows_flowrun WHERE id = $1;`, r3ID).Columns(map[string]interface{}{"status": "W"})
}
