package msgio_test

import (
	"encoding/json"
	"testing"

	"github.com/gomodule/redigo/redis"
	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/msgio"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueueCourierMessages(t *testing.T) {
	ctx, rt, db, rp := testsuite.Get()
	rc := rp.Get()
	defer rc.Close()

	defer testsuite.Reset(testsuite.ResetAll)

	// create an Andoid channel
	androidChannel := testdata.InsertChannel(db, testdata.Org1, "A", "Android 1", []string{"tel"}, "SR", map[string]interface{}{"FCM_ID": "FCMID"})

	oa, err := models.GetOrgAssetsWithRefresh(ctx, rt, testdata.Org1.ID, models.RefreshOrg|models.RefreshChannels)
	require.NoError(t, err)

	tests := []struct {
		Description string
		Msgs        []msgSpec
		QueueSizes  map[string][]int
	}{
		{
			Description: "2 queueable messages",
			Msgs: []msgSpec{
				{
					ChannelID: testdata.TwilioChannel.ID,
					ContactID: testdata.Cathy.ID,
					URNID:     testdata.Cathy.URNID,
				},
				{
					ChannelID: testdata.TwilioChannel.ID,
					ContactID: testdata.Cathy.ID,
					URNID:     testdata.Cathy.URNID,
				},
			},
			QueueSizes: map[string][]int{
				"msgs:74729f45-7f29-4868-9dc4-90e491e3c7d8|10/0": {2},
			},
		},
		{
			Description: "1 queueable message and 1 failed",
			Msgs: []msgSpec{
				{
					ChannelID: testdata.TwilioChannel.ID,
					ContactID: testdata.Cathy.ID,
					URNID:     testdata.Cathy.URNID,
					Failed:    true,
				},
				{
					ChannelID: testdata.TwilioChannel.ID,
					ContactID: testdata.Cathy.ID,
					URNID:     testdata.Cathy.URNID,
				},
			},
			QueueSizes: map[string][]int{
				"msgs:74729f45-7f29-4868-9dc4-90e491e3c7d8|10/0": {1},
			},
		},
		{
			Description: "0 messages",
			Msgs:        []msgSpec{},
			QueueSizes:  map[string][]int{},
		},
	}

	for _, tc := range tests {
		var contactID models.ContactID
		msgs := make([]*models.Msg, len(tc.Msgs))
		for i, ms := range tc.Msgs {
			msgs[i] = ms.createMsg(t, rt, oa)
			contactID = ms.ContactID
		}

		rc.Do("FLUSHDB")
		msgio.QueueCourierMessages(rc, contactID, msgs)

		testsuite.AssertCourierQueues(t, tc.QueueSizes, "courier queue sizes mismatch in '%s'", tc.Description)
	}

	// check that trying to queue a courier message will panic
	assert.Panics(t, func() {
		ms := msgSpec{
			ChannelID: androidChannel.ID,
			ContactID: testdata.Cathy.ID,
			URNID:     testdata.Cathy.URNID,
		}
		msgio.QueueCourierMessages(rc, testdata.Cathy.ID, []*models.Msg{ms.createMsg(t, rt, oa)})
	})
}

func TestPushCourierBatch(t *testing.T) {
	ctx, rt, _, rp := testsuite.Get()
	rc := rp.Get()
	defer rc.Close()

	defer testsuite.Reset(testsuite.ResetData | testsuite.ResetRedis)

	oa, err := models.GetOrgAssetsWithRefresh(ctx, rt, testdata.Org1.ID, models.RefreshChannels)
	require.NoError(t, err)

	channel := oa.ChannelByID(testdata.TwilioChannel.ID)

	msg1 := (&msgSpec{ChannelID: testdata.TwilioChannel.ID, ContactID: testdata.Cathy.ID, URNID: testdata.Cathy.URNID}).createMsg(t, rt, oa)
	msg2 := (&msgSpec{ChannelID: testdata.TwilioChannel.ID, ContactID: testdata.Cathy.ID, URNID: testdata.Cathy.URNID}).createMsg(t, rt, oa)

	err = msgio.PushCourierBatch(rc, channel, []*models.Msg{msg1, msg2}, "1636557205.123456")
	require.NoError(t, err)

	// check that channel has been added to active list
	msgsActive, err := redis.Strings(rc.Do("ZRANGE", "msgs:active", 0, -1))
	assert.NoError(t, err)
	assert.Equal(t, []string{"msgs:74729f45-7f29-4868-9dc4-90e491e3c7d8|10"}, msgsActive)

	// and that msgs were added as single batch to bulk priority (0) queue
	queued, err := redis.ByteSlices(rc.Do("ZRANGE", "msgs:74729f45-7f29-4868-9dc4-90e491e3c7d8|10/0", 0, -1))
	assert.NoError(t, err)
	assert.Equal(t, 1, len(queued))

	unmarshaled, err := jsonx.DecodeGeneric(queued[0])
	assert.NoError(t, err)
	assert.Equal(t, 2, len(unmarshaled.([]interface{})))

	item1ID, _ := unmarshaled.([]interface{})[0].(map[string]interface{})["id"].(json.Number).Int64()
	item2ID, _ := unmarshaled.([]interface{})[1].(map[string]interface{})["id"].(json.Number).Int64()
	assert.Equal(t, int64(msg1.ID()), item1ID)
	assert.Equal(t, int64(msg2.ID()), item2ID)

	// push another batch in the same epoch second with transaction counter still below limit
	rc.Do("SET", "msgs:74729f45-7f29-4868-9dc4-90e491e3c7d8|10:tps:1636557205", "5")

	msg3 := (&msgSpec{ChannelID: testdata.TwilioChannel.ID, ContactID: testdata.Cathy.ID, URNID: testdata.Cathy.URNID}).createMsg(t, rt, oa)

	err = msgio.PushCourierBatch(rc, channel, []*models.Msg{msg3}, "1636557205.234567")
	require.NoError(t, err)

	queued, err = redis.ByteSlices(rc.Do("ZRANGE", "msgs:74729f45-7f29-4868-9dc4-90e491e3c7d8|10/0", 0, -1))
	assert.NoError(t, err)
	assert.Equal(t, 2, len(queued))

	// simulate channel having been throttled
	rc.Do("ZREM", "msgs:active", "msgs:74729f45-7f29-4868-9dc4-90e491e3c7d8|10")
	rc.Do("SET", "msgs:74729f45-7f29-4868-9dc4-90e491e3c7d8|10:tps:1636557205", "11")

	msg4 := (&msgSpec{ChannelID: testdata.TwilioChannel.ID, ContactID: testdata.Cathy.ID, URNID: testdata.Cathy.URNID}).createMsg(t, rt, oa)

	err = msgio.PushCourierBatch(rc, channel, []*models.Msg{msg4}, "1636557205.345678")
	require.NoError(t, err)

	// check that channel has *not* been added to active list
	msgsActive, err = redis.Strings(rc.Do("ZRANGE", "msgs:active", 0, -1))
	assert.NoError(t, err)
	assert.Equal(t, []string{}, msgsActive)

	// but msg was still added to queue
	queued, err = redis.ByteSlices(rc.Do("ZRANGE", "msgs:74729f45-7f29-4868-9dc4-90e491e3c7d8|10/0", 0, -1))
	assert.NoError(t, err)
	assert.Equal(t, 3, len(queued))
}
