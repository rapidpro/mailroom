package msgs_test

import (
	"testing"

	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/queue"
	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/nyaruka/mailroom/core/tasks/msgs"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSendMsg(t *testing.T) {
	_, rt := testsuite.Runtime()
	rc := rt.RP.Get()
	defer rc.Close()

	defer testsuite.Reset(testsuite.ResetData | testsuite.ResetRedis)

	tcs := []struct {
		org                  *testdata.Org
		task                 *msgs.SendMsgTask
		expectedStatus       models.MsgStatus
		expectedFailedReason models.MsgFailedReason
	}{
		{
			task: &msgs.SendMsgTask{
				ContactID: testdata.Cathy.ID,
				Text:      "hi there",
			},
			expectedStatus: models.MsgStatusQueued,
		},
	}

	for _, tc := range tcs {
		err := tasks.Queue(rc, "handler", testdata.Org1.ID, tc.task, queue.DefaultPriority)
		require.NoError(t, err)

		testsuite.FlushTasks(t, rt)

		msg := &struct {
			ContactID    models.ContactID       `db:"contact_id"`
			Text         string                 `db:"text"`
			Status       models.MsgStatus       `db:"status"`
			FailedReason models.MsgFailedReason `db:"failed_reason"`
			Direction    string                 `db:"direction"`
		}{}
		err = rt.DB.Get(msg, `SELECT contact_id, text, status, failed_reason, direction FROM msgs_msg ORDER BY id DESC LIMIT 1`)
		require.NoError(t, err)

		assert.Equal(t, tc.task.ContactID, msg.ContactID)
		assert.Equal(t, tc.task.Text, msg.Text)
		assert.Equal(t, tc.expectedStatus, msg.Status)
		assert.Equal(t, tc.expectedFailedReason, msg.FailedReason)
		assert.Equal(t, "O", msg.Direction)
	}
}
