package msgio_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/nyaruka/gocommon/i18n"
	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/test"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/msgio"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/nyaruka/null/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewCourierMsg(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetData)

	// create an opt-in and a new contact with an auth token for it
	optInID := testdata.InsertOptIn(rt, testdata.Org1, "Joke Of The Day").ID
	testFred := testdata.InsertContact(rt, testdata.Org1, "", "Fred", "eng", models.ContactStatusActive)
	testdata.InsertContactURN(rt, testdata.Org1, testFred, "tel:+593979123456", 1000, map[string]string{fmt.Sprintf("optin:%d", optInID): "sesame"})

	oa, err := models.GetOrgAssetsWithRefresh(ctx, rt, testdata.Org1.ID, models.RefreshOptIns)
	require.NoError(t, err)
	require.False(t, oa.Org().Suspended())

	_, cathy, cathyURNs := testdata.Cathy.Load(rt, oa)
	_, fred, fredURNs := testFred.Load(rt, oa)

	channel := oa.ChannelByUUID(testdata.TwilioChannel.UUID)
	flow, _ := oa.FlowByID(testdata.Favorites.ID)
	optIn := oa.OptInByID(optInID)
	cathyURN, _ := cathyURNs[0].AsURN(oa)
	fredURN, _ := fredURNs[0].AsURN(oa)

	tpl := oa.TemplateByUUID("9c22b594-fcab-4b29-9bcb-ce4404894a80")

	flowMsg1 := flows.NewMsgOut(
		cathyURN,
		assets.NewChannelReference(testdata.TwilioChannel.UUID, "Test Channel"),
		"Hi there",
		[]utils.Attachment{utils.Attachment("image/jpeg:https://dl-foo.com/image.jpg")},
		[]string{"yes", "no"},
		flows.NewMsgTemplating(assets.NewTemplateReference("9c22b594-fcab-4b29-9bcb-ce4404894a80", "revive_issue"), []string{"name"}, "tpls"),
		flows.MsgTopicPurchase,
		`eng-US`,
		flows.NilUnsendableReason,
	)

	// create a non-priority flow message.. i.e. the session isn't responding to an incoming message
	testdata.InsertWaitingSession(rt, testdata.Org1, testdata.Cathy, models.FlowTypeMessaging, testdata.Favorites, models.NilCallID, time.Now(), time.Now(), false, nil)
	session, err := models.FindWaitingSessionForContact(ctx, rt.DB, rt.SessionStorage, oa, models.FlowTypeMessaging, cathy)
	require.NoError(t, err)

	msg1, err := models.NewOutgoingFlowMsg(rt, oa.Org(), channel, session, flow, flowMsg1, tpl, time.Date(2021, 11, 9, 14, 3, 30, 0, time.UTC))
	require.NoError(t, err)

	createAndAssertCourierMsg(t, ctx, rt, oa, msg1, cathyURNs[0], fmt.Sprintf(`{
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
				"template": {"uuid": "9c22b594-fcab-4b29-9bcb-ce4404894a80", "name": "revive_issue"},
				"variables": ["name"],
				"namespace": "tpls",
				"language": "en_US"
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
		"urn": "tel:+16055741111",
		"uuid": "%s"
	}`, session.ID(), msg1.UUID()))

	// create a priority flow message.. i.e. the session is responding to an incoming message
	cathy.SetLastSeenOn(time.Date(2023, 4, 20, 10, 15, 0, 0, time.UTC))
	flowMsg2 := flows.NewMsgOut(
		cathyURN,
		assets.NewChannelReference(testdata.TwilioChannel.UUID, "Test Channel"),
		"Hi there",
		nil, nil, nil,
		flows.NilMsgTopic,
		i18n.NilLocale,
		flows.NilUnsendableReason,
	)
	in1 := testdata.InsertIncomingMsg(rt, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "test", models.MsgStatusHandled)
	session.SetIncomingMsg(in1.ID, null.String("EX123"))
	msg2, err := models.NewOutgoingFlowMsg(rt, oa.Org(), channel, session, flow, flowMsg2, nil, time.Date(2021, 11, 9, 14, 3, 30, 0, time.UTC))
	require.NoError(t, err)

	createAndAssertCourierMsg(t, ctx, rt, oa, msg2, cathyURNs[0], fmt.Sprintf(`{
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
		"urn": "tel:+16055741111",
		"uuid": "%s"
	}`, session.ID(), msg2.UUID()))

	// try a broadcast message which won't have session and flow fields set and won't be high priority
	bcastID := testdata.InsertBroadcast(rt, testdata.Org1, `eng`, map[i18n.Language]string{`eng`: "Blast"}, nil, models.NilScheduleID, []*testdata.Contact{testFred}, nil)
	bcastMsg1 := flows.NewMsgOut(fredURN, assets.NewChannelReference(testdata.TwilioChannel.UUID, "Test Channel"), "Blast", nil, nil, nil, flows.NilMsgTopic, i18n.NilLocale, flows.NilUnsendableReason)
	msg3, err := models.NewOutgoingBroadcastMsg(rt, oa.Org(), channel, fred, bcastMsg1, time.Date(2021, 11, 9, 14, 3, 30, 0, time.UTC), &models.BroadcastBatch{BroadcastID: bcastID, OptInID: optInID, CreatedByID: testdata.Admin.ID})
	require.NoError(t, err)

	createAndAssertCourierMsg(t, ctx, rt, oa, msg3, fredURNs[0], fmt.Sprintf(`{
		"channel_uuid": "74729f45-7f29-4868-9dc4-90e491e3c7d8",
		"contact_id": 30000,
		"contact_urn_id": 30000,
		"created_on": "2021-11-09T14:03:30Z",
		"high_priority": false,
		"id": 4,
		"org_id": 1,
		"origin": "broadcast",
		"text": "Blast",
		"tps_cost": 1,
		"urn": "tel:+593979123456",
		"urn_auth": "sesame",
		"uuid": "%s"
	}`, msg3.UUID()))

	msg4 := models.NewOutgoingOptInMsg(rt, session, flow, optIn, channel, "tel:+16055741111?id=10000", time.Date(2021, 11, 9, 14, 3, 30, 0, time.UTC))

	createAndAssertCourierMsg(t, ctx, rt, oa, msg4, cathyURNs[0], fmt.Sprintf(`{
		"channel_uuid": "74729f45-7f29-4868-9dc4-90e491e3c7d8",
		"contact_id": 10000,
		"contact_urn_id": 10000,
		"created_on": "2021-11-09T14:03:30Z",
		"flow": {"uuid": "9de3663f-c5c5-4c92-9f45-ecbc09abcc85", "name": "Favorites"},
		"high_priority": true,
		"id": 5,
		"optin": {
			"id": %d,
			"name": "Joke Of The Day"
		},
		"org_id": 1,
		"origin": "flow",
		"response_to_external_id": "EX123",
		"session_id": %d,
		"session_status": "W",
		"text": "",
		"tps_cost": 1,
		"urn": "tel:+16055741111",
		"uuid": "%s"
	}`, optIn.ID(), session.ID(), msg4.UUID()))
}

func createAndAssertCourierMsg(t *testing.T, ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, m *models.Msg, u *models.ContactURN, expectedJSON string) {
	// insert to db so that it gets an id
	err := models.InsertMessages(ctx, rt.DB, []*models.Msg{m})
	require.NoError(t, err)

	channel := oa.ChannelByID(m.ChannelID())

	cmsg3, err := msgio.NewCourierMsg(oa, m, u, channel)
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

	_, _, cathyURNs := testdata.Cathy.Load(rt, oa)
	twilio := oa.ChannelByUUID(testdata.TwilioChannel.UUID)

	// noop if no messages provided
	msgio.QueueCourierMessages(rc, oa, testdata.Cathy.ID, twilio, []msgio.Send{})
	testsuite.AssertCourierQueues(t, map[string][]int{})

	// queue 3 messages for Cathy..
	sends := []msgio.Send{
		{
			Msg: (&msgSpec{Channel: testdata.TwilioChannel, Contact: testdata.Cathy}).createMsg(t, rt, oa),
			URN: cathyURNs[0],
		},
		{
			Msg: (&msgSpec{Channel: testdata.TwilioChannel, Contact: testdata.Cathy}).createMsg(t, rt, oa),
			URN: cathyURNs[0],
		},
		{
			Msg: (&msgSpec{Channel: testdata.TwilioChannel, Contact: testdata.Cathy, HighPriority: true}).createMsg(t, rt, oa),
			URN: cathyURNs[0],
		},
	}

	msgio.QueueCourierMessages(rc, oa, testdata.Cathy.ID, twilio, sends)

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

	_, _, cathyURNs := testdata.Cathy.Load(rt, oa)
	twilio := oa.ChannelByUUID(testdata.TwilioChannel.UUID)
	vonage := oa.ChannelByUUID(testdata.VonageChannel.UUID)

	// queue 3 Twilio messages for Cathy..
	msgio.QueueCourierMessages(rc, oa, testdata.Cathy.ID, twilio, []msgio.Send{
		{
			Msg: (&msgSpec{Channel: testdata.TwilioChannel, Contact: testdata.Cathy}).createMsg(t, rt, oa),
			URN: cathyURNs[0],
		},
		{
			Msg: (&msgSpec{Channel: testdata.TwilioChannel, Contact: testdata.Cathy}).createMsg(t, rt, oa),
			URN: cathyURNs[0],
		},
		{
			Msg: (&msgSpec{Channel: testdata.TwilioChannel, Contact: testdata.Cathy, HighPriority: true}).createMsg(t, rt, oa),
			URN: cathyURNs[0],
		},
	})

	// and a Vonage message
	msgio.QueueCourierMessages(rc, oa, testdata.Cathy.ID, vonage, []msgio.Send{
		{
			Msg: (&msgSpec{Channel: testdata.VonageChannel, Contact: testdata.Cathy}).createMsg(t, rt, oa),
			URN: cathyURNs[0],
		},
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

	_, _, cathyURNs := testdata.Cathy.Load(rt, oa)
	channel := oa.ChannelByID(testdata.TwilioChannel.ID)

	msg1 := (&msgSpec{Channel: testdata.TwilioChannel, Contact: testdata.Cathy}).createMsg(t, rt, oa)
	msg2 := (&msgSpec{Channel: testdata.TwilioChannel, Contact: testdata.Cathy}).createMsg(t, rt, oa)

	err = msgio.PushCourierBatch(rc, oa, channel, []msgio.Send{{msg1, cathyURNs[0]}, {msg2, cathyURNs[0]}}, "1636557205.123456")
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
	assert.Equal(t, 2, len(unmarshaled.([]any)))

	item1ID, _ := unmarshaled.([]any)[0].(map[string]any)["id"].(json.Number).Int64()
	item2ID, _ := unmarshaled.([]any)[1].(map[string]any)["id"].(json.Number).Int64()
	assert.Equal(t, int64(msg1.ID()), item1ID)
	assert.Equal(t, int64(msg2.ID()), item2ID)

	// push another batch in the same epoch second with transaction counter still below limit
	rc.Do("SET", "msgs:74729f45-7f29-4868-9dc4-90e491e3c7d8|10:tps:1636557205", "5")

	msg3 := (&msgSpec{Channel: testdata.TwilioChannel, Contact: testdata.Cathy}).createMsg(t, rt, oa)

	err = msgio.PushCourierBatch(rc, oa, channel, []msgio.Send{{msg3, cathyURNs[0]}}, "1636557205.234567")
	require.NoError(t, err)

	queued, err = redis.ByteSlices(rc.Do("ZRANGE", "msgs:74729f45-7f29-4868-9dc4-90e491e3c7d8|10/0", 0, -1))
	assert.NoError(t, err)
	assert.Equal(t, 2, len(queued))

	// simulate channel having been throttled
	rc.Do("ZREM", "msgs:active", "msgs:74729f45-7f29-4868-9dc4-90e491e3c7d8|10")
	rc.Do("SET", "msgs:74729f45-7f29-4868-9dc4-90e491e3c7d8|10:tps:1636557205", "11")

	msg4 := (&msgSpec{Channel: testdata.TwilioChannel, Contact: testdata.Cathy}).createMsg(t, rt, oa)

	err = msgio.PushCourierBatch(rc, oa, channel, []msgio.Send{{msg4, cathyURNs[0]}}, "1636557205.345678")
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
