package handler

import (
	"testing"
	"time"

	"github.com/nyaruka/gocommon/uuids"

	_ "github.com/nyaruka/mailroom/hooks"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/queue"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/stretchr/testify/assert"
)

func TestRetryMsgs(t *testing.T) {
	testsuite.Reset()
	db := testsuite.DB()
	rp := testsuite.RP()
	ctx := testsuite.CTX()

	rc := rp.Get()
	defer rc.Close()

	// noop does nothing
	err := retryPendingMsgs(ctx, db, rp, "test", "test")
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
		db.MustExec(
			`INSERT INTO msgs_msg(uuid, org_id, channel_id, contact_id, contact_urn_id, text, direction, status, created_on, visibility, msg_count, error_count, next_attempt) 
						   VALUES($1,   $2,     $3,         $4,         $5,             $6,   $7,        $8,     $9,         'V',        1,         0,           NOW())`,
			uuids.New(), models.Org1, models.TwilioChannelID, models.CathyID, models.CathyURNID, msg.Text, models.DirectionIn, msg.Status, msg.CreatedOn)
	}

	err = retryPendingMsgs(ctx, db, rp, "test", "test")
	assert.NoError(t, err)

	// should have one message requeued
	task, _ := queue.PopNextTask(rc, queue.HandlerQueue)
	assert.NotNil(t, task)
	err = handleContactEvent(ctx, db, rp, task)
	assert.NoError(t, err)

	// message should be handled now
	testsuite.AssertQueryCount(t, db, `SELECT count(*) from msgs_msg WHERE text = 'pending' AND status = 'H'`, []interface{}{}, 1)

	// only one message was queued
	task, _ = queue.PopNextTask(rc, queue.HandlerQueue)
	assert.Nil(t, task)
}
