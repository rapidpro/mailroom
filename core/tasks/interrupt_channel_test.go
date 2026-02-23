package tasks_test

import (
	"testing"
	"time"

	"github.com/nyaruka/gocommon/dbutil/assertdb"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/core/crons"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInterruptChannel(t *testing.T) {
	ctx, rt := testsuite.Runtime(t)

	defer testsuite.Reset(t, rt, testsuite.ResetData|testsuite.ResetDynamo|testsuite.ResetValkey)

	// twilio call
	twilioCall := testdb.InsertCall(t, rt, testdb.Org1, testdb.TwilioChannel, testdb.Dan)

	// vonage call
	vonageCall := testdb.InsertCall(t, rt, testdb.Org1, testdb.VonageChannel, testdb.Cat)

	sessionUUID1 := testdb.InsertWaitingSession(t, rt, testdb.Org1, testdb.Ann, models.FlowTypeMessaging, nil, testdb.Favorites)
	sessionUUID2 := testdb.InsertWaitingSession(t, rt, testdb.Org1, testdb.Cat, models.FlowTypeVoice, vonageCall, testdb.Favorites)
	sessionUUID3 := testdb.InsertWaitingSession(t, rt, testdb.Org1, testdb.Dan, models.FlowTypeVoice, twilioCall, testdb.Favorites)

	rt.DB.MustExec(`UPDATE ivr_call SET session_uuid = $2 WHERE id = $1`, vonageCall.ID, sessionUUID2)
	rt.DB.MustExec(`UPDATE ivr_call SET session_uuid = $2 WHERE id = $1`, twilioCall.ID, sessionUUID3)

	testdb.InsertOutgoingMsg(t, rt, testdb.Org1, "0199bad8-f98d-75a3-b641-2718a25ac3f5", testdb.TwilioChannel, testdb.Ann, "how can we help", nil, models.MsgStatusPending, false)
	testdb.InsertOutgoingMsg(t, rt, testdb.Org1, "0199bad9-9791-770d-a47d-8f4a6ea3ad13", testdb.VonageChannel, testdb.Bob, "this failed", nil, models.MsgStatusQueued, false)
	testdb.InsertOutgoingMsg(t, rt, testdb.Org1, "0199bb93-ec0f-703e-9b5b-d26d4b6b133c", testdb.VonageChannel, testdb.Cat, "no URN", nil, models.MsgStatusPending, false)
	testdb.InsertOutgoingMsg(t, rt, testdb.Org1, "0199bb94-1134-75d6-91dc-8aee7787f703", testdb.VonageChannel, testdb.Cat, "no URN", nil, models.MsgStatusErrored, false)
	testdb.InsertOutgoingMsg(t, rt, testdb.Org1, "0199bb96-3c4c-72f2-bacc-4b6ae4c592b3", testdb.VonageChannel, testdb.Cat, "no URN", nil, models.MsgStatusFailed, false)

	assertdb.Query(t, rt.DB, `SELECT status FROM flows_flowsession WHERE uuid = $1`, sessionUUID1).Returns("W")
	assertdb.Query(t, rt.DB, `SELECT status FROM flows_flowsession WHERE uuid = $1`, sessionUUID2).Returns("W")
	assertdb.Query(t, rt.DB, `SELECT status FROM flows_flowsession WHERE uuid = $1`, sessionUUID3).Returns("W")

	assertdb.Query(t, rt.DB, `SELECT count(*) FROM msgs_msg WHERE status = 'F' and failed_reason = 'R' and channel_id = $1`, testdb.VonageChannel.ID).Returns(0)
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM msgs_msg WHERE status = 'F' and channel_id = $1`, testdb.VonageChannel.ID).Returns(1)
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM msgs_msg WHERE status = 'F' and channel_id = $1`, testdb.TwilioChannel.ID).Returns(0)

	// get current modified_on for Cat so we can check it gets updated
	var catModifiedOn1 time.Time
	require.NoError(t, rt.DB.Get(&catModifiedOn1, `SELECT modified_on FROM contacts_contact WHERE id = $1`, testdb.Cat.ID))

	// queue and perform a task to interrupt the Twilio channel
	tasks.Queue(ctx, rt, rt.Queues.Batch, testdb.Org1.ID, &tasks.InterruptChannel{ChannelID: testdb.TwilioChannel.ID}, false)
	testsuite.FlushTasks(t, rt)

	assertdb.Query(t, rt.DB, `SELECT count(*) FROM msgs_msg WHERE status = 'F' and channel_id = $1`, testdb.VonageChannel.ID).Returns(1)
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM msgs_msg WHERE status = 'F' and failed_reason = 'R' and channel_id = $1`, testdb.VonageChannel.ID).Returns(0)
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM msgs_msg WHERE status = 'F' and failed_reason = 'R' and channel_id = $1`, testdb.TwilioChannel.ID).Returns(1)

	assertdb.Query(t, rt.DB, `SELECT status FROM flows_flowsession WHERE uuid = $1`, sessionUUID1).Returns("W")
	assertdb.Query(t, rt.DB, `SELECT status FROM flows_flowsession WHERE uuid = $1`, sessionUUID2).Returns("W")
	assertdb.Query(t, rt.DB, `SELECT status FROM flows_flowsession WHERE uuid = $1`, sessionUUID3).Returns("I")

	testdb.InsertErroredOutgoingMsg(t, rt, testdb.Org1, testdb.TwilioChannel, testdb.Ann, "Hi", 1, time.Now().Add(-time.Hour), false)
	testdb.InsertErroredOutgoingMsg(t, rt, testdb.Org1, testdb.VonageChannel, testdb.Bob, "Hi", 2, time.Now().Add(-time.Minute), false)
	testdb.InsertErroredOutgoingMsg(t, rt, testdb.Org1, testdb.VonageChannel, testdb.Bob, "Hi", 2, time.Now().Add(-time.Minute), false)
	testdb.InsertErroredOutgoingMsg(t, rt, testdb.Org1, testdb.VonageChannel, testdb.Bob, "Hi", 2, time.Now().Add(-time.Minute), true) // high priority

	// just to create courier queues
	cron := &crons.RetrySendingCron{}
	_, err := cron.Run(ctx, rt)
	require.NoError(t, err)

	testsuite.AssertCourierQueues(t, rt, map[string][]int{
		"msgs:74729f45-7f29-4868-9dc4-90e491e3c7d8|10/0": {1}, // twilio, bulk priority
		"msgs:19012bfd-3ce3-4cae-9bb9-76cf92c73d49|10/0": {2}, // vonage, bulk priority
		"msgs:19012bfd-3ce3-4cae-9bb9-76cf92c73d49|10/1": {1}, // vonage, high priority
	})

	// queue and perform a task to interrupt the Vonage channel
	tasks.Queue(ctx, rt, rt.Queues.Batch, testdb.Org1.ID, &tasks.InterruptChannel{ChannelID: testdb.VonageChannel.ID}, false)
	testsuite.FlushTasks(t, rt)

	assertdb.Query(t, rt.DB, `SELECT count(*) FROM msgs_msg WHERE status = 'F' and failed_reason = 'R' and channel_id = $1`, testdb.VonageChannel.ID).Returns(6)
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM msgs_msg WHERE status = 'F' and channel_id = $1`, testdb.VonageChannel.ID).Returns(7)
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM msgs_msg WHERE status = 'F' and failed_reason = 'R' and channel_id = $1`, testdb.TwilioChannel.ID).Returns(1)

	assertdb.Query(t, rt.DB, `SELECT status FROM flows_flowsession WHERE uuid = $1`, sessionUUID1).Returns("W")
	assertdb.Query(t, rt.DB, `SELECT status FROM flows_flowsession WHERE uuid = $1`, sessionUUID2).Returns("I")
	assertdb.Query(t, rt.DB, `SELECT status FROM flows_flowsession WHERE uuid = $1`, sessionUUID3).Returns("I")

	// vonage queues should be cleared
	testsuite.AssertCourierQueues(t, rt, map[string][]int{
		"msgs:74729f45-7f29-4868-9dc4-90e491e3c7d8|10/0": {1}, // twilio, bulk priority
	})

	// check that run ended events were persisted for Cat and Dan
	assert.Equal(t, map[flows.ContactUUID][]string{
		testdb.Cat.UUID: {"run_ended"},
		testdb.Dan.UUID: {"run_ended"},
	}, testsuite.GetHistoryEventTypes(t, rt, false, time.Time{}))

	var catModifiedOn2 time.Time
	require.NoError(t, rt.DB.Get(&catModifiedOn2, `SELECT modified_on FROM contacts_contact WHERE id = $1`, testdb.Cat.ID))
	assert.Greater(t, catModifiedOn2, catModifiedOn1)
}
