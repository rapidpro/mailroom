package interrupts_test

import (
	"testing"
	"time"

	"github.com/nyaruka/gocommon/dbutil/assertdb"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/queue"
	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/nyaruka/mailroom/core/tasks/interrupts"
	"github.com/nyaruka/mailroom/core/tasks/msgs"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/stretchr/testify/require"
)

func TestInterruptChannel(t *testing.T) {
	ctx, rt := testsuite.Runtime()
	rc := rt.RP.Get()
	defer rc.Close()

	defer testsuite.Reset(testsuite.ResetData | testsuite.ResetRedis)

	insertSession := func(org *testdata.Org, contact *testdata.Contact, flow *testdata.Flow, connectionID models.CallID) models.SessionID {
		sessionID := testdata.InsertWaitingSession(rt, org, contact, models.FlowTypeMessaging, flow, connectionID, time.Now(), time.Now(), false, nil)

		// give session one waiting run too
		testdata.InsertFlowRun(rt, org, sessionID, contact, flow, models.RunStatusWaiting)
		return sessionID
	}

	// twilio call
	twilioCallID := testdata.InsertCall(rt, testdata.Org1, testdata.TwilioChannel, testdata.Alexandria)

	// vonage call
	vonageCallID := testdata.InsertCall(rt, testdata.Org1, testdata.VonageChannel, testdata.George)

	sessionID1 := insertSession(testdata.Org1, testdata.Cathy, testdata.Favorites, models.NilCallID)
	sessionID2 := insertSession(testdata.Org1, testdata.George, testdata.Favorites, vonageCallID)
	sessionID3 := insertSession(testdata.Org1, testdata.Alexandria, testdata.Favorites, twilioCallID)

	testdata.InsertOutgoingMsg(rt, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "how can we help", nil, models.MsgStatusPending, false)
	testdata.InsertOutgoingMsg(rt, testdata.Org1, testdata.VonageChannel, testdata.Bob, "this failed", nil, models.MsgStatusQueued, false)
	testdata.InsertOutgoingMsg(rt, testdata.Org1, testdata.VonageChannel, testdata.George, "no URN", nil, models.MsgStatusPending, false)
	testdata.InsertOutgoingMsg(rt, testdata.Org1, testdata.VonageChannel, testdata.George, "no URN", nil, models.MsgStatusErrored, false)
	testdata.InsertOutgoingMsg(rt, testdata.Org1, testdata.VonageChannel, testdata.George, "no URN", nil, models.MsgStatusFailed, false)

	assertdb.Query(t, rt.DB, `SELECT status FROM flows_flowsession WHERE id = $1`, sessionID1).Returns("W")
	assertdb.Query(t, rt.DB, `SELECT status FROM flows_flowsession WHERE id = $1`, sessionID2).Returns("W")
	assertdb.Query(t, rt.DB, `SELECT status FROM flows_flowsession WHERE id = $1`, sessionID3).Returns("W")

	assertdb.Query(t, rt.DB, `SELECT count(*) FROM msgs_msg WHERE status = 'F' and failed_reason = 'R' and channel_id = $1`, testdata.VonageChannel.ID).Returns(0)
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM msgs_msg WHERE status = 'F' and channel_id = $1`, testdata.VonageChannel.ID).Returns(1)
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM msgs_msg WHERE status = 'F' and channel_id = $1`, testdata.TwilioChannel.ID).Returns(0)

	// queue and perform a task to interrupt the Twilio channel
	tasks.Queue(rc, queue.BatchQueue, testdata.Org1.ID, &interrupts.InterruptChannelTask{ChannelID: testdata.TwilioChannel.ID}, queue.DefaultPriority)
	testsuite.FlushTasks(t, rt)

	assertdb.Query(t, rt.DB, `SELECT count(*) FROM msgs_msg WHERE status = 'F' and channel_id = $1`, testdata.VonageChannel.ID).Returns(1)
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM msgs_msg WHERE status = 'F' and failed_reason = 'R' and channel_id = $1`, testdata.VonageChannel.ID).Returns(0)
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM msgs_msg WHERE status = 'F' and failed_reason = 'R' and channel_id = $1`, testdata.TwilioChannel.ID).Returns(1)

	assertdb.Query(t, rt.DB, `SELECT status FROM flows_flowsession WHERE id = $1`, sessionID1).Returns("W")
	assertdb.Query(t, rt.DB, `SELECT status FROM flows_flowsession WHERE id = $1`, sessionID2).Returns("W")
	assertdb.Query(t, rt.DB, `SELECT status FROM flows_flowsession WHERE id = $1`, sessionID3).Returns("I")

	testdata.InsertErroredOutgoingMsg(rt, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "Hi", 1, time.Now().Add(-time.Hour), false)
	testdata.InsertErroredOutgoingMsg(rt, testdata.Org1, testdata.VonageChannel, testdata.Bob, "Hi", 2, time.Now().Add(-time.Minute), false)
	testdata.InsertErroredOutgoingMsg(rt, testdata.Org1, testdata.VonageChannel, testdata.Bob, "Hi", 2, time.Now().Add(-time.Minute), false)
	testdata.InsertErroredOutgoingMsg(rt, testdata.Org1, testdata.VonageChannel, testdata.Bob, "Hi", 2, time.Now().Add(-time.Minute), true) // high priority

	// just to create courier queues
	cron := &msgs.RetryMessagesCron{}
	_, err := cron.Run(ctx, rt)
	require.NoError(t, err)

	testsuite.AssertCourierQueues(t, map[string][]int{
		"msgs:74729f45-7f29-4868-9dc4-90e491e3c7d8|10/0": {1}, // twilio, bulk priority
		"msgs:19012bfd-3ce3-4cae-9bb9-76cf92c73d49|10/0": {2}, // vonage, bulk priority
		"msgs:19012bfd-3ce3-4cae-9bb9-76cf92c73d49|10/1": {1}, // vonage, high priority
	})

	// queue and perform a task to interrupt the Vonage channel
	tasks.Queue(rc, queue.BatchQueue, testdata.Org1.ID, &interrupts.InterruptChannelTask{ChannelID: testdata.VonageChannel.ID}, queue.DefaultPriority)
	testsuite.FlushTasks(t, rt)

	assertdb.Query(t, rt.DB, `SELECT count(*) FROM msgs_msg WHERE status = 'F' and failed_reason = 'R' and channel_id = $1`, testdata.VonageChannel.ID).Returns(6)
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM msgs_msg WHERE status = 'F' and channel_id = $1`, testdata.VonageChannel.ID).Returns(7)
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM msgs_msg WHERE status = 'F' and failed_reason = 'R' and channel_id = $1`, testdata.TwilioChannel.ID).Returns(1)

	assertdb.Query(t, rt.DB, `SELECT status FROM flows_flowsession WHERE id = $1`, sessionID1).Returns("W")
	assertdb.Query(t, rt.DB, `SELECT status FROM flows_flowsession WHERE id = $1`, sessionID2).Returns("I")
	assertdb.Query(t, rt.DB, `SELECT status FROM flows_flowsession WHERE id = $1`, sessionID3).Returns("I")

	// vonage queues should be cleared
	testsuite.AssertCourierQueues(t, map[string][]int{
		"msgs:74729f45-7f29-4868-9dc4-90e491e3c7d8|10/0": {1}, // twilio, bulk priority
	})

}
