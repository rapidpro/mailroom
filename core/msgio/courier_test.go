package msgio_test

import (
	"fmt"
	"testing"
	"time"

	valkey "github.com/gomodule/redigo/redis"
	"github.com/nyaruka/gocommon/i18n"
	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/goflow/flows/triggers"
	"github.com/nyaruka/goflow/test"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/msgio"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewCourierMsg(t *testing.T) {
	ctx, rt := testsuite.Runtime(t)

	defer testsuite.Reset(t, rt, testsuite.ResetData|testsuite.ResetValkey|testsuite.ResetDynamo)

	// create an opt-in and a new contact with an auth token for it
	optInID := testdb.InsertOptIn(t, rt, testdb.Org1, "45aec4dd-945f-4511-878f-7d8516fbd336", "Joke Of The Day").ID
	testFred := testdb.InsertContact(t, rt, testdb.Org1, "fed2d179-73ac-44fd-b838-7f866fef0a3a", "Fred", "eng", models.ContactStatusActive)
	testdb.InsertContactURN(t, rt, testdb.Org1, testFred, "tel:+593979123456", 1000, map[string]string{fmt.Sprintf("optin:%d", optInID): "sesame"})

	oa, err := models.GetOrgAssetsWithRefresh(ctx, rt, testdb.Org1.ID, models.RefreshOptIns)
	require.NoError(t, err)
	require.False(t, oa.Org().Suspended())

	ann, _, annURNs := testdb.Ann.Load(t, rt, oa)
	fred, _, fredURNs := testFred.Load(t, rt, oa)

	twilio := oa.ChannelByUUID(testdb.TwilioChannel.UUID)
	facebook := oa.ChannelByUUID(testdb.FacebookChannel.UUID)
	flow, _ := oa.FlowByID(testdb.Favorites.ID)
	optIn := oa.OptInByID(optInID)
	annURN, _ := annURNs[0].Encode(oa)
	fredURN, _ := fredURNs[0].Encode(oa)

	scenes := testsuite.StartSessions(t, rt, oa, []*testdb.Contact{testdb.Ann}, triggers.NewBuilder(testdb.Favorites.Reference()).Manual().Build())
	session, sprint := scenes[0].Session, scenes[0].Sprint

	msgEvent1 := events.NewMsgCreated(flows.NewMsgOut(
		annURN,
		assets.NewChannelReference(testdb.FacebookChannel.UUID, "Facebook"),
		&flows.MsgContent{
			Text:         "Hi there",
			Attachments:  []utils.Attachment{utils.Attachment("image/jpeg:https://dl-foo.com/image.jpg")},
			QuickReplies: []flows.QuickReply{{Type: "text", Text: "yes", Extra: "if you really want"}, {Type: "text", Text: "no"}},
		},
		flows.NewMsgTemplating(
			assets.NewTemplateReference(testdb.ReviveTemplate.UUID, "revive_issue"),
			[]*flows.TemplatingComponent{{Type: "body", Name: "body", Variables: map[string]int{"1": 0}}},
			[]*flows.TemplatingVariable{{Type: "text", Value: "name"}},
		),
		`eng-US`,
		"",
	), "", "")

	msg1, err := models.NewOutgoingFlowMsg(rt, oa.Org(), facebook, ann, flow, msgEvent1, nil)
	require.NoError(t, err)

	// insert to db so that it gets an id and time field values
	err = models.InsertMessages(ctx, rt.DB, []*models.Msg{msg1.Msg})
	require.NoError(t, err)

	msg1.URN = annURNs[0]
	msg1.Session = session
	msg1.SprintUUID = sprint.UUID()

	createAndAssertCourierMsg(t, oa, msg1, fmt.Sprintf(`{
		"attachments": [
			"image/jpeg:https://dl-foo.com/image.jpg"
		],
		"channel_uuid": "0f661e8b-ea9d-4bd3-9953-d368340acf91",
		"contact": {"id": 10000, "uuid": "a393abc0-283d-4c9b-a1b3-641a035c34bf"},
		"created_on": %s,
		"flow": {"uuid": "9de3663f-c5c5-4c92-9f45-ecbc09abcc85", "name": "Favorites"},
		"high_priority": false,
		"locale": "eng-US",
		"org_id": 1,
		"origin": "flow",
		"quick_replies": [{"type": "text", "text": "yes", "extra": "if you really want"}, {"type": "text", "text": "no"}],
		"session": {
			"uuid": "%s",
			"status": "W",
			"sprint_uuid": "%s"
        },
		"templating": {
			"template": {"uuid": "9c22b594-fcab-4b29-9bcb-ce4404894a80", "name": "revive_issue"},
			"components": [{"type": "body", "name": "body", "variables": {"1": 0}}],
			"variables": [{"type": "text", "value": "name"}],
			"namespace": "2d40b45c_25cd_4965_9019_f05d0124c5fa",
			"external_id": "eng1",
			"language": "en_US"
		},
		"text": "Hi there",
		"tps_cost": 2,
		"urn": "tel:+16055741111",
		"uuid": "%s"
	}`, string(jsonx.MustMarshal(msgEvent1.CreatedOn().In(time.UTC))), session.UUID(), sprint.UUID(), msg1.UUID()))

	// create a priority flow message.. i.e. the session is responding to an incoming message
	ann.UpdateLastSeenOn(ctx, rt.DB, time.Date(2023, 4, 20, 10, 15, 0, 0, time.UTC))
	msgEvent2 := events.NewMsgCreated(flows.NewMsgOut(
		annURN,
		assets.NewChannelReference(testdb.TwilioChannel.UUID, "Test Channel"),
		&flows.MsgContent{Text: "Hi there"},
		nil,
		i18n.NilLocale,
		"",
	), "", "")
	in1 := testdb.InsertIncomingMsg(t, rt, testdb.Org1, "0199bad8-f98d-75a3-b641-2718a25ac3f5", testdb.TwilioChannel, testdb.Ann, "test", models.MsgStatusHandled)
	msg2, err := models.NewOutgoingFlowMsg(rt, oa.Org(), twilio, ann, flow, msgEvent2, &models.MsgInRef{UUID: in1.UUID, ExtID: "EX123"})
	require.NoError(t, err)

	err = models.InsertMessages(ctx, rt.DB, []*models.Msg{msg2.Msg})
	require.NoError(t, err)

	msg2.URN = annURNs[0]
	msg2.Session = session
	msg2.SprintUUID = sprint.UUID()

	createAndAssertCourierMsg(t, oa, msg2, fmt.Sprintf(`{
		"channel_uuid": "74729f45-7f29-4868-9dc4-90e491e3c7d8",
		"contact": {"id": 10000, "uuid": "a393abc0-283d-4c9b-a1b3-641a035c34bf", "last_seen_on": "2023-04-20T10:15:00Z"},
		"created_on": %s,
		"flow": {"uuid": "9de3663f-c5c5-4c92-9f45-ecbc09abcc85", "name": "Favorites"},
		"response_to_external_id": "EX123",
		"high_priority": true,
		"org_id": 1,
		"origin": "flow",
		"session": {
			"uuid": "%s",
			"status": "W",
			"sprint_uuid": "%s"
        },
		"text": "Hi there",
		"tps_cost": 1,
		"urn": "tel:+16055741111",
		"uuid": "%s"
	}`, string(jsonx.MustMarshal(msgEvent2.CreatedOn().In(time.UTC))), session.UUID(), sprint.UUID(), msg2.UUID()))

	// try a broadcast message which won't have session and flow fields set and won't be high priority
	bcast := testdb.InsertBroadcast(t, rt, testdb.Org1, "0199877e-0ed2-790b-b474-35099cea401c", `eng`, map[i18n.Language]string{`eng`: "Blast"}, nil, models.NilScheduleID, []*testdb.Contact{testFred}, nil)
	msgEvent3 := events.NewMsgCreated(
		flows.NewMsgOut(fredURN, assets.NewChannelReference(testdb.TwilioChannel.UUID, "Test Channel"), &flows.MsgContent{Text: "Blast"}, nil, i18n.NilLocale, ""),
		bcast.UUID,
		"",
	)
	msg3, err := models.NewOutgoingBroadcastMsg(rt, oa.Org(), twilio, fred, msgEvent3, &models.Broadcast{ID: bcast.ID, OptInID: optInID, CreatedByID: testdb.Admin.ID})
	require.NoError(t, err)

	err = models.InsertMessages(ctx, rt.DB, []*models.Msg{msg3.Msg})
	require.NoError(t, err)

	msg3.URN = fredURNs[0]

	createAndAssertCourierMsg(t, oa, msg3, fmt.Sprintf(`{
		"channel_uuid": "74729f45-7f29-4868-9dc4-90e491e3c7d8",
		"contact": {"id": 30000, "uuid": "fed2d179-73ac-44fd-b838-7f866fef0a3a"},
		"created_on": %s,
		"high_priority": false,
		"org_id": 1,
		"origin": "broadcast",
		"text": "Blast",
		"tps_cost": 1,
		"urn": "tel:+593979123456",
		"urn_auth": "sesame",
		"user_id": %d,
		"uuid": "%s"
	}`, string(jsonx.MustMarshal(msgEvent3.CreatedOn().In(time.UTC))), testdb.Admin.ID, msg3.UUID()))

	optInEvent := events.NewOptInRequested(session.Assets().OptIns().Get(optIn.UUID()).Reference(), twilio.Reference(), "tel:+16055741111")
	msg4 := models.NewOutgoingOptInMsg(rt, testdb.Org1.ID, ann, flow, optIn, twilio, optInEvent, &models.MsgInRef{UUID: in1.UUID, ExtID: "EX123"})
	err = models.InsertMessages(ctx, rt.DB, []*models.Msg{msg4.Msg})
	require.NoError(t, err)

	msg4.URN = annURNs[0]
	msg4.Session = session
	msg4.SprintUUID = sprint.UUID()

	createAndAssertCourierMsg(t, oa, msg4, fmt.Sprintf(`{
		"channel_uuid": "74729f45-7f29-4868-9dc4-90e491e3c7d8",
		"contact": {"id": 10000, "uuid": "a393abc0-283d-4c9b-a1b3-641a035c34bf", "last_seen_on": "2023-04-20T10:15:00Z"},
		"created_on": %s,
		"flow": {"uuid": "9de3663f-c5c5-4c92-9f45-ecbc09abcc85", "name": "Favorites"},
		"high_priority": true,
		"optin": {
			"id": %d,
			"name": "Joke Of The Day"
		},
		"org_id": 1,
		"origin": "flow",
		"response_to_external_id": "EX123",
		"session": {
			"uuid": "%s",
			"status": "W",
			"sprint_uuid": "%s"
        },
		"text": "",
		"tps_cost": 1,
		"urn": "tel:+16055741111",
		"uuid": "%s"
	}`, string(jsonx.MustMarshal(optInEvent.CreatedOn().In(time.UTC))), optIn.ID(), session.UUID(), sprint.UUID(), msg4.UUID()))

	// make msg1 look like it errored and fetch it for retrying
	rt.DB.MustExec(`UPDATE msgs_msg SET status = 'E', error_count = 1, next_attempt = $2 WHERE id = $1`, msg1.ID(), time.Now())

	msgs, err := models.GetMessagesForRetry(ctx, rt.DB)
	assert.NoError(t, err)

	retries, err := models.PrepareMessagesForRetry(ctx, rt.DB, msgs)
	assert.NoError(t, err)
	assert.Len(t, retries, 1)

	createAndAssertCourierMsg(t, oa, retries[0], fmt.Sprintf(`{
		"attachments": [
			"image/jpeg:https://dl-foo.com/image.jpg"
		],
		"channel_uuid": "0f661e8b-ea9d-4bd3-9953-d368340acf91",
		"contact": {"id": 10000, "last_seen_on": "2023-04-20T10:15:00Z", "uuid": "a393abc0-283d-4c9b-a1b3-641a035c34bf"},
		"created_on": %s,
		"flow": {"uuid": "9de3663f-c5c5-4c92-9f45-ecbc09abcc85", "name": "Favorites"},
		"high_priority": false,
		"locale": "eng-US",
		"org_id": 1,
		"origin": "flow",
		"prev_attempts": 1,
		"quick_replies": [{"type": "text", "text": "yes", "extra": "if you really want"}, {"type": "text", "text": "no"}],
		"templating": {
			"template": {"uuid": "9c22b594-fcab-4b29-9bcb-ce4404894a80", "name": "revive_issue"},
			"components": [{"type": "body", "name": "body", "variables": {"1": 0}}],
			"variables": [{"type": "text", "value": "name"}],
			"namespace": "2d40b45c_25cd_4965_9019_f05d0124c5fa",
			"external_id": "eng1",
			"language": "en_US"
		},
		"text": "Hi there",
		"tps_cost": 2,
		"urn": "tel:+16055741111",
		"uuid": "%s"
	}`, string(jsonx.MustMarshal(msgEvent1.CreatedOn().In(time.UTC).Round(time.Microsecond))), msg1.UUID()))
}

func createAndAssertCourierMsg(t *testing.T, oa *models.OrgAssets, msg *models.MsgOut, expectedJSON string) {
	channel := oa.ChannelByID(msg.ChannelID())

	cmsg3, err := msgio.NewCourierMsg(oa, msg, channel)
	assert.NoError(t, err)

	marshaled := jsonx.MustMarshal(cmsg3)

	test.AssertEqualJSON(t, []byte(expectedJSON), marshaled)
}

func TestQueueCourierMessages(t *testing.T) {
	ctx, rt := testsuite.Runtime(t)
	vc := rt.VK.Get()
	defer vc.Close()

	defer testsuite.Reset(t, rt, testsuite.ResetData|testsuite.ResetValkey)

	oa, err := models.GetOrgAssetsWithRefresh(ctx, rt, testdb.Org1.ID, models.RefreshOrg|models.RefreshChannels)
	require.NoError(t, err)

	ann, _, annURNs := testdb.Ann.Load(t, rt, oa)
	twilio := oa.ChannelByUUID(testdb.TwilioChannel.UUID)

	// noop if no messages provided
	msgio.QueueCourierMessages(vc, oa, testdb.Ann.ID, twilio, []*models.MsgOut{})
	testsuite.AssertCourierQueues(t, rt, map[string][]int{})

	// queue 3 messages for Ann..
	sends := []*models.MsgOut{
		{
			Msg:     (&msgSpec{Channel: testdb.TwilioChannel, Contact: testdb.Ann}).createMsg(t, rt, oa),
			URN:     annURNs[0],
			Contact: ann,
		},
		{
			Msg:     (&msgSpec{Channel: testdb.TwilioChannel, Contact: testdb.Ann}).createMsg(t, rt, oa),
			URN:     annURNs[0],
			Contact: ann,
		},
		{
			Msg:     (&msgSpec{Channel: testdb.TwilioChannel, Contact: testdb.Ann, HighPriority: true}).createMsg(t, rt, oa),
			URN:     annURNs[0],
			Contact: ann,
		},
	}

	msgio.QueueCourierMessages(vc, oa, testdb.Ann.ID, twilio, sends)

	testsuite.AssertCourierQueues(t, rt, map[string][]int{
		"msgs:74729f45-7f29-4868-9dc4-90e491e3c7d8|10/0": {2}, // twilio, bulk priority
		"msgs:74729f45-7f29-4868-9dc4-90e491e3c7d8|10/1": {1}, // twilio, high priority
	})
}

func TestClearChannelCourierQueue(t *testing.T) {
	ctx, rt := testsuite.Runtime(t)
	vc := rt.VK.Get()
	defer vc.Close()

	defer testsuite.Reset(t, rt, testsuite.ResetData|testsuite.ResetValkey)

	oa, err := models.GetOrgAssetsWithRefresh(ctx, rt, testdb.Org1.ID, models.RefreshOrg|models.RefreshChannels)
	require.NoError(t, err)

	ann, _, annURNs := testdb.Ann.Load(t, rt, oa)
	twilio := oa.ChannelByUUID(testdb.TwilioChannel.UUID)
	vonage := oa.ChannelByUUID(testdb.VonageChannel.UUID)

	// queue 3 Twilio messages for Ann..
	msgio.QueueCourierMessages(vc, oa, testdb.Ann.ID, twilio, []*models.MsgOut{
		{
			Msg:     (&msgSpec{Channel: testdb.TwilioChannel, Contact: testdb.Ann}).createMsg(t, rt, oa),
			URN:     annURNs[0],
			Contact: ann,
		},
		{
			Msg:     (&msgSpec{Channel: testdb.TwilioChannel, Contact: testdb.Ann}).createMsg(t, rt, oa),
			URN:     annURNs[0],
			Contact: ann,
		},
		{
			Msg:     (&msgSpec{Channel: testdb.TwilioChannel, Contact: testdb.Ann, HighPriority: true}).createMsg(t, rt, oa),
			URN:     annURNs[0],
			Contact: ann,
		},
	})

	// and a Vonage message
	msgio.QueueCourierMessages(vc, oa, testdb.Ann.ID, vonage, []*models.MsgOut{
		{
			Msg:     (&msgSpec{Channel: testdb.VonageChannel, Contact: testdb.Ann}).createMsg(t, rt, oa),
			URN:     annURNs[0],
			Contact: ann,
		},
	})

	testsuite.AssertCourierQueues(t, rt, map[string][]int{
		"msgs:74729f45-7f29-4868-9dc4-90e491e3c7d8|10/0": {2}, // twilio, bulk priority
		"msgs:74729f45-7f29-4868-9dc4-90e491e3c7d8|10/1": {1}, // twilio, high priority
		"msgs:19012bfd-3ce3-4cae-9bb9-76cf92c73d49|10/0": {1}, // vonage, bulk priority
	})

	twilioChannel := oa.ChannelByID(testdb.TwilioChannel.ID)
	msgio.ClearCourierQueues(vc, twilioChannel)

	testsuite.AssertCourierQueues(t, rt, map[string][]int{
		"msgs:19012bfd-3ce3-4cae-9bb9-76cf92c73d49|10/0": {1}, // vonage, bulk priority
	})

	vonageChannel := oa.ChannelByID(testdb.VonageChannel.ID)
	msgio.ClearCourierQueues(vc, vonageChannel)
	testsuite.AssertCourierQueues(t, rt, map[string][]int{})

}

func TestPushCourierBatch(t *testing.T) {
	ctx, rt := testsuite.Runtime(t)
	vc := rt.VK.Get()
	defer vc.Close()

	defer testsuite.Reset(t, rt, testsuite.ResetData|testsuite.ResetValkey)

	oa, err := models.GetOrgAssetsWithRefresh(ctx, rt, testdb.Org1.ID, models.RefreshChannels)
	require.NoError(t, err)

	ann, _, annURNs := testdb.Ann.Load(t, rt, oa)
	channel := oa.ChannelByID(testdb.TwilioChannel.ID)

	msg1 := (&msgSpec{Channel: testdb.TwilioChannel, Contact: testdb.Ann}).createMsg(t, rt, oa)
	msg2 := (&msgSpec{Channel: testdb.TwilioChannel, Contact: testdb.Ann}).createMsg(t, rt, oa)

	err = msgio.PushCourierBatch(vc, oa, channel, []*models.MsgOut{{Msg: msg1, URN: annURNs[0], Contact: ann}, {Msg: msg2, URN: annURNs[0], Contact: ann}}, "1636557205.123456")
	require.NoError(t, err)

	// check that channel has been added to active list
	msgsActive, err := valkey.Strings(vc.Do("ZRANGE", "msgs:active", 0, -1))
	assert.NoError(t, err)
	assert.Equal(t, []string{"msgs:74729f45-7f29-4868-9dc4-90e491e3c7d8|10"}, msgsActive)

	// and that msgs were added as single batch to bulk priority (0) queue
	queued, err := valkey.ByteSlices(vc.Do("ZRANGE", "msgs:74729f45-7f29-4868-9dc4-90e491e3c7d8|10/0", 0, -1))
	assert.NoError(t, err)
	assert.Equal(t, 1, len(queued))

	unmarshaled, err := jsonx.DecodeGeneric(queued[0])
	assert.NoError(t, err)
	assert.Equal(t, 2, len(unmarshaled.([]any)))

	item1UUID := unmarshaled.([]any)[0].(map[string]any)["uuid"].(string)
	item2UUID := unmarshaled.([]any)[1].(map[string]any)["uuid"].(string)
	assert.Equal(t, msg1.UUID(), flows.EventUUID(item1UUID))
	assert.Equal(t, msg2.UUID(), flows.EventUUID(item2UUID))

	// push another batch in the same epoch second with transaction counter still below limit
	vc.Do("SET", "msgs:74729f45-7f29-4868-9dc4-90e491e3c7d8|10:tps:1636557205", "5")

	msg3 := (&msgSpec{Channel: testdb.TwilioChannel, Contact: testdb.Ann}).createMsg(t, rt, oa)

	err = msgio.PushCourierBatch(vc, oa, channel, []*models.MsgOut{{Msg: msg3, URN: annURNs[0], Contact: ann}}, "1636557205.234567")
	require.NoError(t, err)

	queued, err = valkey.ByteSlices(vc.Do("ZRANGE", "msgs:74729f45-7f29-4868-9dc4-90e491e3c7d8|10/0", 0, -1))
	assert.NoError(t, err)
	assert.Equal(t, 2, len(queued))

	// simulate channel having been throttled
	vc.Do("ZREM", "msgs:active", "msgs:74729f45-7f29-4868-9dc4-90e491e3c7d8|10")
	vc.Do("SET", "msgs:74729f45-7f29-4868-9dc4-90e491e3c7d8|10:tps:1636557205", "11")

	msg4 := (&msgSpec{Channel: testdb.TwilioChannel, Contact: testdb.Ann}).createMsg(t, rt, oa)

	err = msgio.PushCourierBatch(vc, oa, channel, []*models.MsgOut{{Msg: msg4, URN: annURNs[0], Contact: ann}}, "1636557205.345678")
	require.NoError(t, err)

	// check that channel has *not* been added to active list
	msgsActive, err = valkey.Strings(vc.Do("ZRANGE", "msgs:active", 0, -1))
	assert.NoError(t, err)
	assert.Equal(t, []string{}, msgsActive)

	// but msg was still added to queue
	queued, err = valkey.ByteSlices(vc.Do("ZRANGE", "msgs:74729f45-7f29-4868-9dc4-90e491e3c7d8|10/0", 0, -1))
	assert.NoError(t, err)
	assert.Equal(t, 3, len(queued))
}
