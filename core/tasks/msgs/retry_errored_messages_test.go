package msgs_test

import (
	"testing"
	"time"

	"github.com/nyaruka/gocommon/dbutil/assertdb"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/tasks/msgs"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/stretchr/testify/assert"
)

func TestRetryErroredMessages(t *testing.T) {
	ctx, rt := testsuite.Runtime()
	rc := rt.RP.Get()
	defer rc.Close()

	defer testsuite.Reset(testsuite.ResetData | testsuite.ResetRedis)

	// nothing to retry
	cron := &msgs.RetryMessagesCron{}
	_, err := cron.Run(ctx, rt)
	assert.NoError(t, err)

	testsuite.AssertCourierQueues(t, map[string][]int{})

	// a non-errored outgoing message (should be ignored)
	testdata.InsertOutgoingMsg(rt, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "Hi", nil, models.MsgStatusDelivered, false)

	// an errored message with a next-attempt in the future (should be ignored)
	testdata.InsertErroredOutgoingMsg(rt, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "Hi", 2, time.Now().Add(time.Hour), false)

	// errored messages with a next-attempt in the past
	testdata.InsertErroredOutgoingMsg(rt, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "Hi", 1, time.Now().Add(-time.Hour), false)
	testdata.InsertErroredOutgoingMsg(rt, testdata.Org1, testdata.VonageChannel, testdata.Bob, "Hi", 2, time.Now().Add(-time.Minute), false)
	msg5 := testdata.InsertErroredOutgoingMsg(rt, testdata.Org1, testdata.VonageChannel, testdata.Bob, "Hi", 2, time.Now().Add(-time.Minute), false)
	testdata.InsertErroredOutgoingMsg(rt, testdata.Org1, testdata.VonageChannel, testdata.Bob, "Hi", 2, time.Now().Add(-time.Minute), true) // high priority

	rt.DB.MustExec(`UPDATE msgs_msg SET status = 'I' WHERE id = $1`, msg5.ID)

	assertdb.Query(t, rt.DB, `SELECT count(*) FROM msgs_msg WHERE status = 'I'`).Returns(1)
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM msgs_msg WHERE status = 'E'`).Returns(4)

	// try again...
	res, err := cron.Run(ctx, rt)
	assert.NoError(t, err)
	assert.Equal(t, map[string]any{"retried": 4}, res)

	assertdb.Query(t, rt.DB, `SELECT count(*) FROM msgs_msg WHERE status = 'D'`).Returns(1)
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM msgs_msg WHERE status = 'E'`).Returns(1)
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM msgs_msg WHERE status = 'Q'`).Returns(4)

	testsuite.AssertCourierQueues(t, map[string][]int{
		"msgs:74729f45-7f29-4868-9dc4-90e491e3c7d8|10/0": {1}, // twilio, bulk priority
		"msgs:19012bfd-3ce3-4cae-9bb9-76cf92c73d49|10/0": {2}, // vonage, bulk priority
		"msgs:19012bfd-3ce3-4cae-9bb9-76cf92c73d49|10/1": {1}, // vonage, high priority
	})
}
