package msgio_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/test"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/msgio"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/nyaruka/null/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewCourierMsg(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetData)

	oa, err := models.GetOrgAssets(ctx, rt, testdata.Org1.ID)
	require.NoError(t, err)
	require.False(t, oa.Org().Suspended())

	_, cathy := testdata.Cathy.Load(rt, oa)

	channel := oa.ChannelByUUID(testdata.TwilioChannel.UUID)
	flow, _ := oa.FlowByID(testdata.Favorites.ID)
	urn := urns.URN(fmt.Sprintf("tel:+250700000001?id=%d", testdata.Cathy.URNID))
	flowMsg1 := flows.NewMsgOut(
		urn,
		assets.NewChannelReference(testdata.TwilioChannel.UUID, "Test Channel"),
		"Hi there",
		[]utils.Attachment{utils.Attachment("image/jpeg:https://dl-foo.com/image.jpg")},
		[]string{"yes", "no"},
		flows.NewMsgTemplating(assets.NewTemplateReference("4474d39c-ac2c-486d-bceb-8a774a515299", "tpl"), []string{"name"}, "tpls"),
		flows.MsgTopicPurchase,
		envs.Locale(`eng-US`),
		flows.NilUnsendableReason,
	)

	// create a non-priority flow message.. i.e. the session isn't responding to an incoming message
	testdata.InsertWaitingSession(rt, testdata.Org1, testdata.Cathy, models.FlowTypeMessaging, testdata.Favorites, models.NilCallID, time.Now(), time.Now(), false, nil)
	session, err := models.FindWaitingSessionForContact(ctx, rt.DB, rt.SessionStorage, oa, models.FlowTypeMessaging, cathy)
	require.NoError(t, err)

	msg1, err := models.NewOutgoingFlowMsg(rt, oa.Org(), channel, session, flow, flowMsg1, time.Date(2021, 11, 9, 14, 3, 30, 0, time.UTC))
	require.NoError(t, err)

	createAndAssertCourierMsg(t, ctx, rt, oa, msg1, fmt.Sprintf(`{
		"attachments": [
			"image/jpeg:https://dl-foo.com/image.jpg"
		],
		"channel_uuid": "74729f45-7f29-4868-9dc4-90e491e3c7d8",
		"contact_id": 10000,
		"contact_urn_id": 10000,
		"created_on": "2021-11-09T14:03:30Z",
		"flow": {"uuid": "9de3663f-c5c5-4c92-9f45-ecbc09abcc85", "name": "Favorites"},
		"high_priority": false,
		"id": 1,
		"locale": "eng-US",
		"metadata": {
			"templating": {
				"namespace": "tpls",
				"template": {"name": "tpl", "uuid": "4474d39c-ac2c-486d-bceb-8a774a515299"},
				"variables": ["name"]
			},
			"topic": "purchase"
		},
		"org_id": 1,
		"origin": "flow",
		"quick_replies": [
			"yes",
			"no"
		],
		"session_id": %d,
		"session_status": "W",
		"text": "Hi there",
		"tps_cost": 2,
		"urn": "tel:+250700000001?id=10000",
		"uuid": "%s"
	}`, session.ID(), msg1.UUID()))

	// create a priority flow message.. i.e. the session is responding to an incoming message
	cathy.SetLastSeenOn(time.Date(2023, 4, 20, 10, 15, 0, 0, time.UTC))
	flowMsg2 := flows.NewMsgOut(
		urn,
		assets.NewChannelReference(testdata.TwilioChannel.UUID, "Test Channel"),
		"Hi there",
		nil, nil, nil,
		flows.NilMsgTopic,
		envs.NilLocale,
		flows.NilUnsendableReason,
	)
	in1 := testdata.InsertIncomingMsg(rt, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "test", models.MsgStatusHandled)
	session.SetIncomingMsg(models.MsgID(in1.ID()), null.String("EX123"))
	msg2, err := models.NewOutgoingFlowMsg(rt, oa.Org(), channel, session, flow, flowMsg2, time.Date(2021, 11, 9, 14, 3, 30, 0, time.UTC))
	require.NoError(t, err)

	createAndAssertCourierMsg(t, ctx, rt, oa, msg2, fmt.Sprintf(`{
		"channel_uuid": "74729f45-7f29-4868-9dc4-90e491e3c7d8",
		"contact_id": 10000,
		"contact_last_seen_on": "2023-04-20T10:15:00Z",
		"contact_urn_id": 10000,
		"created_on": "2021-11-09T14:03:30Z",
		"flow": {"uuid": "9de3663f-c5c5-4c92-9f45-ecbc09abcc85", "name": "Favorites"},
		"response_to_external_id": "EX123",
		"high_priority": true,
		"id": 3,
		"org_id": 1,
		"origin": "flow",
		"session_id": %d,
		"session_status": "W",
		"text": "Hi there",
		"tps_cost": 1,
		"urn": "tel:+250700000001?id=10000",
		"uuid": "%s"
	}`, session.ID(), msg2.UUID()))

	// try a broadcast message which won't have session and flow fields set and won't be high priority
	bcastID := testdata.InsertBroadcast(rt, testdata.Org1, `eng`, map[envs.Language]string{`eng`: "Blast"}, models.NilScheduleID, []*testdata.Contact{testdata.Cathy}, nil)
	bcastMsg1 := flows.NewMsgOut(urn, assets.NewChannelReference(testdata.TwilioChannel.UUID, "Test Channel"), "Blast", nil, nil, nil, flows.NilMsgTopic, envs.NilLocale, flows.NilUnsendableReason)
	msg3, err := models.NewOutgoingBroadcastMsg(rt, oa.Org(), channel, cathy, bcastMsg1, time.Date(2021, 11, 9, 14, 3, 30, 0, time.UTC), &models.BroadcastBatch{BroadcastID: bcastID, CreatedByID: testdata.Admin.ID})
	require.NoError(t, err)

	createAndAssertCourierMsg(t, ctx, rt, oa, msg3, fmt.Sprintf(`{
		"channel_uuid": "74729f45-7f29-4868-9dc4-90e491e3c7d8",
		"contact_id": 10000,
		"contact_last_seen_on": "2023-04-20T10:15:00Z",
		"contact_urn_id": 10000,
		"created_on": "2021-11-09T14:03:30Z",
		"high_priority": false,
		"id": 4,
		"org_id": 1,
		"origin": "broadcast",
		"text": "Blast",
		"tps_cost": 1,
		"urn": "tel:+250700000001?id=10000",
		"uuid": "%s"
	}`, msg3.UUID()))
}

func createAndAssertCourierMsg(t *testing.T, ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, m *models.Msg, expectedJSON string) {
	// insert to db so that it gets an id
	err := models.InsertMessages(ctx, rt.DB, []*models.Msg{m})
	require.NoError(t, err)

	channel := oa.ChannelByID(m.ChannelID())

	cmsg3, err := msgio.NewCourierMsg(oa, m, channel)
	assert.NoError(t, err)

	marshaled := jsonx.MustMarshal(cmsg3)

	test.AssertEqualJSON(t, []byte(expectedJSON), marshaled)
}

func TestQueueCourierMessages(t *testing.T) {
	ctx, rt := testsuite.Runtime()
	rc := rt.RP.Get()
	defer rc.Close()

	defer testsuite.Reset(testsuite.ResetData | testsuite.ResetRedis)

	oa, err := models.GetOrgAssetsWithRefresh(ctx, rt, testdata.Org1.ID, models.RefreshOrg|models.RefreshChannels)
	require.NoError(t, err)

	twilio := oa.ChannelByUUID(testdata.TwilioChannel.UUID)

	// noop if no messages provided
	msgio.QueueCourierMessages(rc, oa, testdata.Cathy.ID, twilio, []*models.Msg{})
	testsuite.AssertCourierQueues(t, map[string][]int{})

	// queue 3 messages for Cathy..
	msgs := []*models.Msg{
		(&msgSpec{Channel: testdata.TwilioChannel, Contact: testdata.Cathy}).createMsg(t, rt, oa),
		(&msgSpec{Channel: testdata.TwilioChannel, Contact: testdata.Cathy}).createMsg(t, rt, oa),
		(&msgSpec{Channel: testdata.TwilioChannel, Contact: testdata.Cathy, HighPriority: true}).createMsg(t, rt, oa),
	}

	msgio.QueueCourierMessages(rc, oa, testdata.Cathy.ID, twilio, msgs)

	testsuite.AssertCourierQueues(t, map[string][]int{
		"msgs:74729f45-7f29-4868-9dc4-90e491e3c7d8|10/0": {2}, // twilio, bulk priority
		"msgs:74729f45-7f29-4868-9dc4-90e491e3c7d8|10/1": {1}, // twilio, high priority
	})
}

func TestClearChannelCourierQueue(t *testing.T) {
	ctx, rt := testsuite.Runtime()
	rc := rt.RP.Get()
	defer rc.Close()

	defer testsuite.Reset(testsuite.ResetData | testsuite.ResetRedis)

	oa, err := models.GetOrgAssetsWithRefresh(ctx, rt, testdata.Org1.ID, models.RefreshOrg|models.RefreshChannels)
	require.NoError(t, err)

	twilio := oa.ChannelByUUID(testdata.TwilioChannel.UUID)
	vonage := oa.ChannelByUUID(testdata.VonageChannel.UUID)

	// queue 3 Twilio messages for Cathy..
	msgio.QueueCourierMessages(rc, oa, testdata.Cathy.ID, twilio, []*models.Msg{
		(&msgSpec{Channel: testdata.TwilioChannel, Contact: testdata.Cathy}).createMsg(t, rt, oa),
		(&msgSpec{Channel: testdata.TwilioChannel, Contact: testdata.Cathy}).createMsg(t, rt, oa),
		(&msgSpec{Channel: testdata.TwilioChannel, Contact: testdata.Cathy, HighPriority: true}).createMsg(t, rt, oa),
	})

	// and a Vonage message
	msgio.QueueCourierMessages(rc, oa, testdata.Cathy.ID, vonage, []*models.Msg{
		(&msgSpec{Channel: testdata.VonageChannel, Contact: testdata.Cathy}).createMsg(t, rt, oa),
	})

	testsuite.AssertCourierQueues(t, map[string][]int{
		"msgs:74729f45-7f29-4868-9dc4-90e491e3c7d8|10/0": {2}, // twilio, bulk priority
		"msgs:74729f45-7f29-4868-9dc4-90e491e3c7d8|10/1": {1}, // twilio, high priority
		"msgs:19012bfd-3ce3-4cae-9bb9-76cf92c73d49|10/0": {1}, // vonage, bulk priority
	})

	twilioChannel := oa.ChannelByID(testdata.TwilioChannel.ID)
	msgio.ClearCourierQueues(rc, twilioChannel)

	testsuite.AssertCourierQueues(t, map[string][]int{
		"msgs:19012bfd-3ce3-4cae-9bb9-76cf92c73d49|10/0": {1}, // vonage, bulk priority
	})

	vonageChannel := oa.ChannelByID(testdata.VonageChannel.ID)
	msgio.ClearCourierQueues(rc, vonageChannel)
	testsuite.AssertCourierQueues(t, map[string][]int{})

}

func TestPushCourierBatch(t *testing.T) {
	ctx, rt := testsuite.Runtime()
	rc := rt.RP.Get()
	defer rc.Close()

	defer testsuite.Reset(testsuite.ResetData | testsuite.ResetRedis)

	oa, err := models.GetOrgAssetsWithRefresh(ctx, rt, testdata.Org1.ID, models.RefreshChannels)
	require.NoError(t, err)

	channel := oa.ChannelByID(testdata.TwilioChannel.ID)

	msg1 := (&msgSpec{Channel: testdata.TwilioChannel, Contact: testdata.Cathy}).createMsg(t, rt, oa)
	msg2 := (&msgSpec{Channel: testdata.TwilioChannel, Contact: testdata.Cathy}).createMsg(t, rt, oa)

	err = msgio.PushCourierBatch(rc, oa, channel, []*models.Msg{msg1, msg2}, "1636557205.123456")
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

	msg3 := (&msgSpec{Channel: testdata.TwilioChannel, Contact: testdata.Cathy}).createMsg(t, rt, oa)

	err = msgio.PushCourierBatch(rc, oa, channel, []*models.Msg{msg3}, "1636557205.234567")
	require.NoError(t, err)

	queued, err = redis.ByteSlices(rc.Do("ZRANGE", "msgs:74729f45-7f29-4868-9dc4-90e491e3c7d8|10/0", 0, -1))
	assert.NoError(t, err)
	assert.Equal(t, 2, len(queued))

	// simulate channel having been throttled
	rc.Do("ZREM", "msgs:active", "msgs:74729f45-7f29-4868-9dc4-90e491e3c7d8|10")
	rc.Do("SET", "msgs:74729f45-7f29-4868-9dc4-90e491e3c7d8|10:tps:1636557205", "11")

	msg4 := (&msgSpec{Channel: testdata.TwilioChannel, Contact: testdata.Cathy}).createMsg(t, rt, oa)

	err = msgio.PushCourierBatch(rc, oa, channel, []*models.Msg{msg4}, "1636557205.345678")
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
