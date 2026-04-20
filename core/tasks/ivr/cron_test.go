package ivr_test

import (
	"testing"

	"github.com/nyaruka/gocommon/dbutil/assertdb"
	"github.com/nyaruka/mailroom/core/ivr"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/queue"
	"github.com/nyaruka/mailroom/core/tasks"
	ivrtasks "github.com/nyaruka/mailroom/core/tasks/ivr"
	"github.com/nyaruka/mailroom/core/tasks/starts"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRetries(t *testing.T) {
	ctx, rt := testsuite.Runtime()
	rc := rt.RP.Get()
	defer rc.Close()

	defer testsuite.Reset(testsuite.ResetAll)

	// register our mock client
	ivr.RegisterServiceType(models.ChannelType("ZZ"), NewMockProvider)

	// update our twilio channel to be of type 'ZZ' and set max_concurrent_events to 1
	rt.DB.MustExec(`UPDATE channels_channel SET channel_type = 'ZZ', config = '{"max_concurrent_events": 1}' WHERE id = $1`, testdata.TwilioChannel.ID)

	// create a flow start for cathy
	start := models.NewFlowStart(testdata.Org1.ID, models.StartTypeTrigger, testdata.IVRFlow.ID).
		WithContactIDs([]models.ContactID{testdata.Cathy.ID})

	err := tasks.Queue(rc, queue.BatchQueue, testdata.Org1.ID, &starts.StartFlowTask{FlowStart: start}, queue.DefaultPriority)
	require.NoError(t, err)

	service.callError = nil
	service.callID = ivr.CallID("call1")

	testsuite.FlushTasks(t, rt)

	assertdb.Query(t, rt.DB, `SELECT COUNT(*) FROM ivr_call WHERE contact_id = $1 AND status = $2 AND external_id = $3`,
		testdata.Cathy.ID, models.CallStatusWired, "call1").Returns(1)

	// change our call to be errored instead of wired
	rt.DB.MustExec(`UPDATE ivr_call SET status = 'E', next_attempt = NOW() WHERE external_id = 'call1';`)

	// fire our retries
	cron := &ivrtasks.RetryCron{}
	res, err := cron.Run(ctx, rt)
	assert.NoError(t, err)
	assert.Equal(t, map[string]any{"retried": 1}, res)

	// should now be in wired state
	assertdb.Query(t, rt.DB, `SELECT COUNT(*) FROM ivr_call WHERE contact_id = $1 AND status = $2 AND external_id = $3`,
		testdata.Cathy.ID, models.CallStatusWired, "call1").Returns(1)

	// back to retry and make the channel inactive
	rt.DB.MustExec(`UPDATE ivr_call SET status = 'E', next_attempt = NOW() WHERE external_id = 'call1';`)
	rt.DB.MustExec(`UPDATE channels_channel SET is_active = FALSE WHERE id = $1`, testdata.TwilioChannel.ID)

	models.FlushCache()
	_, err = cron.Run(ctx, rt)
	assert.NoError(t, err)

	// this time should be failed
	assertdb.Query(t, rt.DB, `SELECT COUNT(*) FROM ivr_call WHERE contact_id = $1 AND status = $2 AND external_id = $3`,
		testdata.Cathy.ID, models.CallStatusFailed, "call1").Returns(1)
}
