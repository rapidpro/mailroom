package msgs_test

import (
	"testing"
	"time"

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

	// a non-errored outgoing message (should be ignored)
	testdata.InsertOutgoingMsg(db, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "Hi", nil, models.MsgStatusDelivered)

	// an errored message with a next-attempt in the future (should be ignored)
	testdata.InsertErroredOutgoingMsg(db, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "Hi", 2, time.Now().Add(time.Hour))

	// errored messages with a next-attempt in the past
	testdata.InsertErroredOutgoingMsg(db, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "Hi", 1, time.Now().Add(-time.Hour))
	testdata.InsertErroredOutgoingMsg(db, testdata.Org1, testdata.VonageChannel, testdata.Bob, "Hi", 2, time.Now().Add(-time.Minute))

	testsuite.AssertQuery(t, db, `SELECT count(*) FROM msgs_msg WHERE status = 'E'`).Returns(3)

	// simulate the celery based task in RapidPro being running
	rc.Do("SET", "celery-task-lock:retry_errored_messages", "32462346262")

	err := msgs.RetryErroredMessages(ctx, rt)
	require.NoError(t, err)

	// no change...
	testsuite.AssertQuery(t, db, `SELECT count(*) FROM msgs_msg WHERE status = 'E'`).Returns(3)

	rc.Do("DEL", "celery-task-lock:retry_errored_messages")

	// try again...
	err = msgs.RetryErroredMessages(ctx, rt)
	require.NoError(t, err)

	testsuite.AssertQuery(t, db, `SELECT count(*) FROM msgs_msg WHERE status = 'D'`).Returns(1)
	testsuite.AssertQuery(t, db, `SELECT count(*) FROM msgs_msg WHERE status = 'E'`).Returns(1)
	testsuite.AssertQuery(t, db, `SELECT count(*) FROM msgs_msg WHERE status = 'Q'`).Returns(2)

	testsuite.AssertCourierQueues(t, map[string][]int{
		"msgs:19012bfd-3ce3-4cae-9bb9-76cf92c73d49|10/0": {1},
		"msgs:74729f45-7f29-4868-9dc4-90e491e3c7d8|10/0": {1},
	})
}
