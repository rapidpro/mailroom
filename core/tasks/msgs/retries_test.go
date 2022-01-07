package msgs_test

import (
	"testing"
	"time"

	"github.com/nyaruka/gocommon/dbutil/assertdb"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/tasks/msgs"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/stretchr/testify/require"
)

func TestRetryErroredMessages(t *testing.T) {
	ctx, rt, db, rp := testsuite.Get()
	rc := rp.Get()
	defer rc.Close()

	defer testsuite.Reset(testsuite.ResetData | testsuite.ResetRedis)

	// nothing to retry
	err := msgs.RetryErroredMessages(ctx, rt)
	require.NoError(t, err)

	testsuite.AssertCourierQueues(t, map[string][]int{})

	// a non-errored outgoing message (should be ignored)
	testdata.InsertOutgoingMsg(db, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "Hi", nil, models.MsgStatusDelivered, false)

	// an errored message with a next-attempt in the future (should be ignored)
	testdata.InsertErroredOutgoingMsg(db, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "Hi", 2, time.Now().Add(time.Hour), false)

	// errored messages with a next-attempt in the past
	testdata.InsertErroredOutgoingMsg(db, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "Hi", 1, time.Now().Add(-time.Hour), false)
	testdata.InsertErroredOutgoingMsg(db, testdata.Org1, testdata.VonageChannel, testdata.Bob, "Hi", 2, time.Now().Add(-time.Minute), false)
	testdata.InsertErroredOutgoingMsg(db, testdata.Org1, testdata.VonageChannel, testdata.Bob, "Hi", 2, time.Now().Add(-time.Minute), false)
	testdata.InsertErroredOutgoingMsg(db, testdata.Org1, testdata.VonageChannel, testdata.Bob, "Hi", 2, time.Now().Add(-time.Minute), true) // high priority

	assertdb.Query(t, db, `SELECT count(*) FROM msgs_msg WHERE status = 'E'`).Returns(5)

	// try again...
	err = msgs.RetryErroredMessages(ctx, rt)
	require.NoError(t, err)

	assertdb.Query(t, db, `SELECT count(*) FROM msgs_msg WHERE status = 'D'`).Returns(1)
	assertdb.Query(t, db, `SELECT count(*) FROM msgs_msg WHERE status = 'E'`).Returns(1)
	assertdb.Query(t, db, `SELECT count(*) FROM msgs_msg WHERE status = 'Q'`).Returns(4)

	testsuite.AssertCourierQueues(t, map[string][]int{
		"msgs:74729f45-7f29-4868-9dc4-90e491e3c7d8|10/0": {1}, // twilio, bulk priority
		"msgs:19012bfd-3ce3-4cae-9bb9-76cf92c73d49|10/0": {2}, // vonage, bulk priority
		"msgs:19012bfd-3ce3-4cae-9bb9-76cf92c73d49|10/1": {1}, // vonage, high priority
	})
}
