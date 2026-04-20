package handler_test

import (
	"testing"
	"time"

	"github.com/nyaruka/gocommon/dbutil/assertdb"
	"github.com/nyaruka/gocommon/uuids"
	_ "github.com/nyaruka/mailroom/core/handlers"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/queue"
	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/nyaruka/mailroom/core/tasks/handler"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/stretchr/testify/assert"
)

func TestRetryMsgs(t *testing.T) {
	ctx, rt := testsuite.Runtime()
	rc := rt.RP.Get()
	defer rc.Close()

	defer testsuite.Reset(testsuite.ResetData | testsuite.ResetRedis)

	// noop does nothing
	cron := handler.NewRetryPendingCron()
	_, err := cron.Run(ctx, rt)
	assert.NoError(t, err)

	testMsgs := []struct {
		Text      string
		Status    models.MsgStatus
		CreatedOn time.Time
	}{
		{"pending", models.MsgStatusPending, time.Now().Add(-time.Hour)},
		{"handled", models.MsgStatusHandled, time.Now().Add(-time.Hour)},
		{"recent", models.MsgStatusPending, time.Now()},
	}

	for _, msg := range testMsgs {
		rt.DB.MustExec(
			`INSERT INTO msgs_msg(uuid, org_id, channel_id, contact_id, contact_urn_id, text, direction, msg_type, status, created_on, visibility, msg_count, error_count, next_attempt) 
						   VALUES($1,   $2,     $3,         $4,         $5,             $6,   $7,        'T',      $8,     $9,         'V',        1,         0,           NOW())`,
			uuids.New(), testdata.Org1.ID, testdata.TwilioChannel.ID, testdata.Cathy.ID, testdata.Cathy.URNID, msg.Text, models.DirectionIn, msg.Status, msg.CreatedOn)
	}

	res, err := cron.Run(ctx, rt)
	assert.NoError(t, err)
	assert.Equal(t, map[string]any{"retried": 1}, res)

	// should have one message requeued
	task, _ := queue.PopNextTask(rc, queue.HandlerQueue)
	assert.NotNil(t, task)
	err = tasks.Perform(ctx, rt, task)
	assert.NoError(t, err)

	// message should be handled now
	assertdb.Query(t, rt.DB, `SELECT count(*) from msgs_msg WHERE text = 'pending' AND status = 'H'`).Returns(1)

	// only one message was queued
	task, _ = queue.PopNextTask(rc, queue.HandlerQueue)
	assert.Nil(t, task)
}
