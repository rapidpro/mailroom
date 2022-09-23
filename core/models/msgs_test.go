package models_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/gocommon/dbutil/assertdb"
	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/test"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/nyaruka/null"
	"github.com/nyaruka/redisx/assertredis"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewOutgoingFlowMsg(t *testing.T) {
	ctx, rt, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetData)

	blake := testdata.InsertContact(db, testdata.Org1, "79b94a23-6d13-43f4-95fe-c733ee457857", "Blake", envs.NilLanguage, models.ContactStatusBlocked)
	blakeURNID := testdata.InsertContactURN(db, testdata.Org1, blake, "tel:++250700000007", 1)

	tcs := []struct {
		ChannelUUID  assets.ChannelUUID
		Text         string
		Contact      *testdata.Contact
		URN          urns.URN
		URNID        models.URNID
		Attachments  []utils.Attachment
		QuickReplies []string
		Topic        flows.MsgTopic
		Unsendable   flows.UnsendableReason
		Flow         *testdata.Flow
		ResponseTo   models.MsgID
		SuspendedOrg bool

		ExpectedStatus       models.MsgStatus
		ExpectedFailedReason models.MsgFailedReason
		ExpectedMetadata     map[string]interface{}
		ExpectedMsgCount     int
		ExpectedPriority     bool
	}{
		{
			ChannelUUID:          "74729f45-7f29-4868-9dc4-90e491e3c7d8",
			Text:                 "missing urn id",
			Contact:              testdata.Cathy,
			URN:                  urns.URN("tel:+250700000001"),
			URNID:                models.URNID(0),
			Flow:                 testdata.Favorites,
			ResponseTo:           models.MsgID(123425),
			ExpectedStatus:       models.MsgStatusQueued,
			ExpectedFailedReason: models.NilMsgFailedReason,
			ExpectedMetadata:     map[string]interface{}{},
			ExpectedMsgCount:     1,
			ExpectedPriority:     true,
		},
		{
			ChannelUUID:          "74729f45-7f29-4868-9dc4-90e491e3c7d8",
			Text:                 "test outgoing",
			Contact:              testdata.Cathy,
			URN:                  urns.URN(fmt.Sprintf("tel:+250700000001?id=%d", testdata.Cathy.URNID)),
			URNID:                testdata.Cathy.URNID,
			QuickReplies:         []string{"yes", "no"},
			Topic:                flows.MsgTopicPurchase,
			Flow:                 testdata.SingleMessage,
			ExpectedStatus:       models.MsgStatusQueued,
			ExpectedFailedReason: models.NilMsgFailedReason,
			ExpectedMetadata: map[string]interface{}{
				"quick_replies": []string{"yes", "no"},
				"topic":         "purchase",
			},
			ExpectedMsgCount: 1,
			ExpectedPriority: false,
		},
		{
			ChannelUUID:          "74729f45-7f29-4868-9dc4-90e491e3c7d8",
			Text:                 "test outgoing",
			Contact:              testdata.Cathy,
			URN:                  urns.URN(fmt.Sprintf("tel:+250700000001?id=%d", testdata.Cathy.URNID)),
			URNID:                testdata.Cathy.URNID,
			Attachments:          []utils.Attachment{utils.Attachment("image/jpeg:https://dl-foo.com/image.jpg")},
			Flow:                 testdata.Favorites,
			ExpectedStatus:       models.MsgStatusQueued,
			ExpectedFailedReason: models.NilMsgFailedReason,
			ExpectedMetadata:     map[string]interface{}{},
			ExpectedMsgCount:     2,
			ExpectedPriority:     false,
		},
		{
			ChannelUUID:          "74729f45-7f29-4868-9dc4-90e491e3c7d8",
			Text:                 "suspended org",
			Contact:              testdata.Cathy,
			URN:                  urns.URN(fmt.Sprintf("tel:+250700000001?id=%d", testdata.Cathy.URNID)),
			URNID:                testdata.Cathy.URNID,
			Flow:                 testdata.Favorites,
			SuspendedOrg:         true,
			ExpectedStatus:       models.MsgStatusFailed,
			ExpectedFailedReason: models.MsgFailedSuspended,
			ExpectedMetadata:     map[string]interface{}{},
			ExpectedMsgCount:     1,
			ExpectedPriority:     false,
		},
		{
			ChannelUUID:          "74729f45-7f29-4868-9dc4-90e491e3c7d8",
			Text:                 "missing URN",
			Contact:              testdata.Cathy,
			URN:                  urns.NilURN,
			URNID:                models.URNID(0),
			Unsendable:           flows.UnsendableReasonNoDestination,
			Flow:                 testdata.Favorites,
			ExpectedStatus:       models.MsgStatusFailed,
			ExpectedFailedReason: models.MsgFailedNoDestination,
			ExpectedMetadata:     map[string]interface{}{},
			ExpectedMsgCount:     1,
			ExpectedPriority:     false,
		},
		{
			ChannelUUID:          "",
			Text:                 "missing Channel",
			Contact:              testdata.Cathy,
			URN:                  urns.URN(fmt.Sprintf("tel:+250700000001?id=%d", testdata.Cathy.URNID)),
			URNID:                testdata.Cathy.URNID,
			Unsendable:           flows.UnsendableReasonNoDestination,
			Flow:                 testdata.Favorites,
			ExpectedStatus:       models.MsgStatusFailed,
			ExpectedFailedReason: models.MsgFailedNoDestination,
			ExpectedMetadata:     map[string]interface{}{},
			ExpectedMsgCount:     1,
			ExpectedPriority:     false,
		},
		{
			ChannelUUID:          "74729f45-7f29-4868-9dc4-90e491e3c7d8",
			Text:                 "blocked contact",
			Contact:              blake,
			URN:                  urns.URN(fmt.Sprintf("tel:+250700000007?id=%d", blakeURNID)),
			URNID:                blakeURNID,
			Unsendable:           flows.UnsendableReasonContactStatus,
			Flow:                 testdata.Favorites,
			ExpectedStatus:       models.MsgStatusFailed,
			ExpectedFailedReason: models.MsgFailedContact,
			ExpectedMetadata:     map[string]interface{}{},
			ExpectedMsgCount:     1,
			ExpectedPriority:     false,
		},
	}

	now := time.Now()

	for _, tc := range tcs {
		desc := fmt.Sprintf("text='%s'", tc.Text)
		db.MustExec(`UPDATE orgs_org SET is_suspended = $1 WHERE id = $2`, tc.SuspendedOrg, testdata.Org1.ID)

		oa, err := models.GetOrgAssetsWithRefresh(ctx, rt, testdata.Org1.ID, models.RefreshOrg)
		require.NoError(t, err)

		channel := oa.ChannelByUUID(tc.ChannelUUID)
		flow, _ := oa.FlowByID(tc.Flow.ID)

		session := insertTestSession(t, ctx, rt, testdata.Org1, tc.Contact, testdata.Favorites)
		if tc.ResponseTo != models.NilMsgID {
			session.SetIncomingMsg(flows.MsgID(tc.ResponseTo), null.NullString)
		}

		flowMsg := flows.NewMsgOut(tc.URN, assets.NewChannelReference(tc.ChannelUUID, "Test Channel"), tc.Text, tc.Attachments, tc.QuickReplies, nil, tc.Topic, tc.Unsendable)
		msg, err := models.NewOutgoingFlowMsg(rt, oa.Org(), channel, session, flow, flowMsg, now)

		assert.NoError(t, err)

		err = models.InsertMessages(ctx, db, []*models.Msg{msg})
		assert.NoError(t, err)
		assert.Equal(t, oa.OrgID(), msg.OrgID())
		assert.Equal(t, tc.Text, msg.Text())
		assert.Equal(t, tc.Contact.ID, msg.ContactID())
		assert.Equal(t, channel, msg.Channel())
		assert.Equal(t, tc.ChannelUUID, msg.ChannelUUID())
		assert.Equal(t, tc.URN, msg.URN())
		if tc.URNID != models.NilURNID {
			assert.Equal(t, tc.URNID, *msg.ContactURNID())
		} else {
			assert.Nil(t, msg.ContactURNID())
		}
		assert.Equal(t, tc.Flow.ID, msg.FlowID())

		assert.Equal(t, tc.ExpectedStatus, msg.Status(), "status mismatch for %s", desc)
		assert.Equal(t, tc.ExpectedFailedReason, msg.FailedReason(), "failed reason mismatch for %s", desc)
		assert.Equal(t, tc.ExpectedMetadata, msg.Metadata())
		assert.Equal(t, tc.ExpectedMsgCount, msg.MsgCount())
		assert.Equal(t, now, msg.CreatedOn())
		assert.True(t, msg.ID() > 0)
		assert.True(t, msg.QueuedOn().After(now))
		assert.True(t, msg.ModifiedOn().After(now))
	}

	// check nil failed reasons are saved as NULLs
	assertdb.Query(t, db, `SELECT count(*) FROM msgs_msg WHERE failed_reason IS NOT NULL`).Returns(4)

	// ensure org is unsuspended
	db.MustExec(`UPDATE orgs_org SET is_suspended = FALSE`)
	models.FlushCache()

	oa, err := models.GetOrgAssetsWithRefresh(ctx, rt, testdata.Org1.ID, models.RefreshOrg)
	require.NoError(t, err)
	channel := oa.ChannelByUUID(testdata.TwilioChannel.UUID)
	flow, _ := oa.FlowByID(testdata.Favorites.ID)
	session := insertTestSession(t, ctx, rt, testdata.Org1, testdata.Cathy, testdata.Favorites)

	// check that msg loop detection triggers after 20 repeats of the same text
	newOutgoing := func(text string) *models.Msg {
		flowMsg := flows.NewMsgOut(urns.URN(fmt.Sprintf("tel:+250700000001?id=%d", testdata.Cathy.URNID)), assets.NewChannelReference(testdata.TwilioChannel.UUID, "Twilio"), text, nil, nil, nil, flows.NilMsgTopic, flows.NilUnsendableReason)
		msg, err := models.NewOutgoingFlowMsg(rt, oa.Org(), channel, session, flow, flowMsg, now)
		require.NoError(t, err)
		return msg
	}

	for i := 0; i < 19; i++ {
		msg := newOutgoing("foo")
		assert.Equal(t, models.MsgStatusQueued, msg.Status())
		assert.Equal(t, models.NilMsgFailedReason, msg.FailedReason())
	}
	for i := 0; i < 10; i++ {
		msg := newOutgoing("foo")
		assert.Equal(t, models.MsgStatusFailed, msg.Status())
		assert.Equal(t, models.MsgFailedLooping, msg.FailedReason())
	}
	for i := 0; i < 5; i++ {
		msg := newOutgoing("bar")
		assert.Equal(t, models.MsgStatusQueued, msg.Status())
		assert.Equal(t, models.NilMsgFailedReason, msg.FailedReason())
	}
}

func TestMarshalMsg(t *testing.T) {
	ctx, rt, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetData)

	assertdb.Query(t, db, `SELECT count(*) FROM orgs_org WHERE is_suspended = TRUE`).Returns(0)

	oa, err := models.GetOrgAssets(ctx, rt, testdata.Org1.ID)
	require.NoError(t, err)
	require.False(t, oa.Org().Suspended())

	channel := oa.ChannelByUUID(testdata.TwilioChannel.UUID)
	flow, _ := oa.FlowByID(testdata.Favorites.ID)
	urn := urns.URN(fmt.Sprintf("tel:+250700000001?id=%d", testdata.Cathy.URNID))
	flowMsg1 := flows.NewMsgOut(
		urn,
		assets.NewChannelReference(testdata.TwilioChannel.UUID, "Test Channel"),
		"Hi there",
		[]utils.Attachment{utils.Attachment("image/jpeg:https://dl-foo.com/image.jpg")},
		[]string{"yes", "no"},
		nil,
		flows.MsgTopicPurchase,
		flows.NilUnsendableReason,
	)

	// create a non-priority flow message.. i.e. the session isn't responding to an incoming message
	session := insertTestSession(t, ctx, rt, testdata.Org1, testdata.Cathy, testdata.Favorites)
	msg1, err := models.NewOutgoingFlowMsg(rt, oa.Org(), channel, session, flow, flowMsg1, time.Date(2021, 11, 9, 14, 3, 30, 0, time.UTC))
	require.NoError(t, err)

	cathy := session.Contact()

	err = models.InsertMessages(ctx, db, []*models.Msg{msg1})
	require.NoError(t, err)

	marshaled, err := json.Marshal(msg1)
	assert.NoError(t, err)

	test.AssertEqualJSON(t, []byte(fmt.Sprintf(`{
		"attachments": [
			"image/jpeg:https://dl-foo.com/image.jpg"
		],
		"channel_id": 10000,
		"channel_uuid": "74729f45-7f29-4868-9dc4-90e491e3c7d8",
		"contact_id": 10000,
		"contact_urn_id": 10000,
		"created_on": "2021-11-09T14:03:30Z",
		"direction": "O",
		"error_count": 0,
		"flow": {"uuid": "9de3663f-c5c5-4c92-9f45-ecbc09abcc85", "name": "Favorites"},
		"high_priority": false,
		"id": %d,
		"metadata": {
			"quick_replies": [
				"yes",
				"no"
			],
			"topic": "purchase"
		},
		"modified_on": %s,
		"next_attempt": null,
		"org_id": 1,
		"queued_on": %s,
		"sent_on": null,
		"session_id": %d,
		"session_status": "W",
		"status": "Q",
		"text": "Hi there",
		"tps_cost": 2,
		"urn": "tel:+250700000001?id=10000",
		"uuid": "%s"
	}`, msg1.ID(), jsonx.MustMarshal(msg1.ModifiedOn()), jsonx.MustMarshal(msg1.QueuedOn()), session.ID(), msg1.UUID())), marshaled)

	// create a priority flow message.. i.e. the session is responding to an incoming message
	flowMsg2 := flows.NewMsgOut(
		urn,
		assets.NewChannelReference(testdata.TwilioChannel.UUID, "Test Channel"),
		"Hi there",
		nil, nil, nil,
		flows.NilMsgTopic,
		flows.NilUnsendableReason,
	)
	in1 := testdata.InsertIncomingMsg(db, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "test", models.MsgStatusHandled)
	session.SetIncomingMsg(flows.MsgID(in1.ID()), null.String("EX123"))
	msg2, err := models.NewOutgoingFlowMsg(rt, oa.Org(), channel, session, flow, flowMsg2, time.Date(2021, 11, 9, 14, 3, 30, 0, time.UTC))
	require.NoError(t, err)

	err = models.InsertMessages(ctx, db, []*models.Msg{msg2})
	require.NoError(t, err)

	marshaled, err = json.Marshal(msg2)
	assert.NoError(t, err)

	test.AssertEqualJSON(t, []byte(fmt.Sprintf(`{
		"channel_id": 10000,
		"channel_uuid": "74729f45-7f29-4868-9dc4-90e491e3c7d8",
		"contact_id": 10000,
		"contact_urn_id": 10000,
		"created_on": "2021-11-09T14:03:30Z",
		"direction": "O",
		"error_count": 0,
		"flow": {"uuid": "9de3663f-c5c5-4c92-9f45-ecbc09abcc85", "name": "Favorites"},
		"response_to_external_id": "EX123",
		"high_priority": true,
		"id": %d,
		"metadata": null,
		"modified_on": %s,
		"next_attempt": null,
		"org_id": 1,
		"queued_on": %s,
		"sent_on": null,
		"session_id": %d,
		"session_status": "W",
		"status": "Q",
		"text": "Hi there",
		"tps_cost": 1,
		"urn": "tel:+250700000001?id=10000",
		"uuid": "%s"
	}`, msg2.ID(), jsonx.MustMarshal(msg2.ModifiedOn()), jsonx.MustMarshal(msg2.QueuedOn()), session.ID(), msg2.UUID())), marshaled)

	// try a broadcast message which won't have session and flow fields set
	bcastID := testdata.InsertBroadcast(db, testdata.Org1, `eng`, map[envs.Language]string{`eng`: "Blast"}, models.NilScheduleID, []*testdata.Contact{testdata.Cathy}, nil)
	bcastMsg1 := flows.NewMsgOut(urn, assets.NewChannelReference(testdata.TwilioChannel.UUID, "Test Channel"), "Blast", nil, nil, nil, flows.NilMsgTopic, flows.NilUnsendableReason)
	msg3, err := models.NewOutgoingBroadcastMsg(rt, oa.Org(), channel, cathy, bcastMsg1, time.Date(2021, 11, 9, 14, 3, 30, 0, time.UTC), bcastID)
	require.NoError(t, err)

	err = models.InsertMessages(ctx, db, []*models.Msg{msg2})
	require.NoError(t, err)

	marshaled, err = json.Marshal(msg3)
	assert.NoError(t, err)

	test.AssertEqualJSON(t, []byte(fmt.Sprintf(`{
		"broadcast_id": %d,
		"channel_id": 10000,
		"channel_uuid": "74729f45-7f29-4868-9dc4-90e491e3c7d8",
		"contact_id": 10000,
		"contact_urn_id": 10000,
		"created_on": "2021-11-09T14:03:30Z",
		"direction": "O",
		"error_count": 0,
		"high_priority": false,
		"id": %d,
		"metadata": null,
		"modified_on": %s,
		"next_attempt": null,
		"org_id": 1,
		"queued_on": %s,
		"sent_on": null,
		"status": "Q",
		"text": "Blast",
		"tps_cost": 1,
		"urn": "tel:+250700000001?id=10000",
		"uuid": "%s"
	}`, bcastID, msg3.ID(), jsonx.MustMarshal(msg3.ModifiedOn()), jsonx.MustMarshal(msg3.QueuedOn()), msg3.UUID())), marshaled)
}

func TestGetMessagesByID(t *testing.T) {
	ctx, _, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetData)

	msgIn1 := testdata.InsertIncomingMsg(db, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "in 1", models.MsgStatusHandled)
	msgOut1 := testdata.InsertOutgoingMsg(db, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "out 1", []utils.Attachment{"image/jpeg:hi.jpg"}, models.MsgStatusSent, false)
	msgOut2 := testdata.InsertOutgoingMsg(db, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "out 2", nil, models.MsgStatusSent, false)
	msgOut3 := testdata.InsertOutgoingMsg(db, testdata.Org2, testdata.Org2Channel, testdata.Org2Contact, "out 3", nil, models.MsgStatusSent, false)
	testdata.InsertOutgoingMsg(db, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "hi 3", nil, models.MsgStatusSent, false)

	ids := []models.MsgID{models.MsgID(msgIn1.ID()), models.MsgID(msgOut1.ID()), models.MsgID(msgOut2.ID()), models.MsgID(msgOut3.ID())}

	msgs, err := models.GetMessagesByID(ctx, db, testdata.Org1.ID, models.DirectionOut, ids)

	// should only return the outgoing messages for this org
	require.NoError(t, err)
	assert.Equal(t, 2, len(msgs))
	assert.Equal(t, "out 1", msgs[0].Text())
	assert.Equal(t, []utils.Attachment{"image/jpeg:hi.jpg"}, msgs[0].Attachments())
	assert.Equal(t, "out 2", msgs[1].Text())

	msgs, err = models.GetMessagesByID(ctx, db, testdata.Org1.ID, models.DirectionIn, ids)

	// should only return the incoming message for this org
	require.NoError(t, err)
	assert.Equal(t, 1, len(msgs))
	assert.Equal(t, "in 1", msgs[0].Text())
}

func TestFailMessages(t *testing.T) {
	ctx, rt, db, rp := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetAll)

	oa, err := models.GetOrgAssets(ctx, rt, testdata.Org1.ID)
	require.NoError(t, err)

	out1 := testdata.InsertOutgoingMsg(db, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "hi", nil, models.MsgStatusPending, false)
	out2 := testdata.InsertOutgoingMsg(db, testdata.Org1, testdata.TwilioChannel, testdata.Bob, "hi", nil, models.MsgStatusErrored, false)
	out3 := testdata.InsertOutgoingMsg(db, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "hi", nil, models.MsgStatusFailed, false)
	out4 := testdata.InsertOutgoingMsg(db, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "hi", nil, models.MsgStatusQueued, false)
	out5 := testdata.InsertOutgoingMsg(db, testdata.Org1, testdata.TwilioChannel, testdata.George, "hi", nil, models.MsgStatusQueued, false)

	ids := []models.MsgID{models.MsgID(out1.ID()), models.MsgID(out2.ID()), models.MsgID(out3.ID()), models.MsgID(out4.ID())}

	msgs, err := models.GetMessagesByID(ctx, db, testdata.Org1.ID, models.DirectionOut, ids)
	require.NoError(t, err)

	now := dates.Now()

	// fail the msgs
	failedMsgs, err := models.FailMessages(ctx, db, rp, oa, msgs)
	require.NoError(t, err)

	assert.Len(t, failedMsgs, 3)

	assertdb.Query(t, db, `SELECT count(*) FROM msgs_msg WHERE status = 'F' AND modified_on > $1`, now).Returns(3)
	assertdb.Query(t, db, `SELECT status FROM msgs_msg WHERE id = $1`, out3.ID()).Columns(map[string]interface{}{"status": "F"})
	assertdb.Query(t, db, `SELECT status FROM msgs_msg WHERE id = $1`, out5.ID()).Columns(map[string]interface{}{"status": "Q"})

}

func TestResendMessages(t *testing.T) {
	ctx, rt, db, rp := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetAll)

	oa, err := models.GetOrgAssets(ctx, rt, testdata.Org1.ID)
	require.NoError(t, err)

	out1 := testdata.InsertOutgoingMsg(db, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "hi", nil, models.MsgStatusFailed, false)
	out2 := testdata.InsertOutgoingMsg(db, testdata.Org1, testdata.TwilioChannel, testdata.Bob, "hi", nil, models.MsgStatusFailed, false)

	// failed message with no channel
	out3 := testdata.InsertOutgoingMsg(db, testdata.Org1, nil, testdata.Cathy, "hi", nil, models.MsgStatusFailed, false)

	// failed message with no URN
	out4 := testdata.InsertOutgoingMsg(db, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "hi", nil, models.MsgStatusFailed, false)
	db.MustExec(`UPDATE msgs_msg SET contact_urn_id = NULL, failed_reason = 'D' WHERE id = $1`, out4.ID())

	// failed message with URN which we no longer have a channel for
	out5 := testdata.InsertOutgoingMsg(db, testdata.Org1, nil, testdata.George, "hi", nil, models.MsgStatusFailed, false)
	db.MustExec(`UPDATE msgs_msg SET failed_reason = 'E' WHERE id = $1`, out5.ID())
	db.MustExec(`UPDATE contacts_contacturn SET scheme = 'viber', path = '1234', identity = 'viber:1234' WHERE id = $1`, testdata.George.URNID)

	// other failed message not included in set to resend
	testdata.InsertOutgoingMsg(db, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "hi", nil, models.MsgStatusFailed, false)

	// give Bob's URN an affinity for the Vonage channel
	db.MustExec(`UPDATE contacts_contacturn SET channel_id = $1 WHERE id = $2`, testdata.VonageChannel.ID, testdata.Bob.URNID)

	ids := []models.MsgID{models.MsgID(out1.ID()), models.MsgID(out2.ID()), models.MsgID(out3.ID()), models.MsgID(out4.ID()), models.MsgID(out5.ID())}
	msgs, err := models.GetMessagesByID(ctx, db, testdata.Org1.ID, models.DirectionOut, ids)
	require.NoError(t, err)

	now := dates.Now()

	// resend both msgs
	resent, err := models.ResendMessages(ctx, db, rp, oa, msgs)
	require.NoError(t, err)

	assert.Len(t, resent, 3) // only #1, #2 and #3 can be resent

	// both messages should now have a channel, a topup and be marked for resending
	assert.True(t, resent[0].IsResend())
	assert.Equal(t, testdata.TwilioChannel.ID, resent[0].ChannelID())
	assert.Equal(t, models.TopupID(1), resent[0].TopupID())
	assert.True(t, resent[1].IsResend())
	assert.Equal(t, testdata.VonageChannel.ID, resent[1].ChannelID()) // channel changed
	assert.Equal(t, models.TopupID(1), resent[1].TopupID())
	assert.True(t, resent[2].IsResend())
	assert.Equal(t, testdata.TwilioChannel.ID, resent[2].ChannelID()) // channel added

	assertdb.Query(t, db, `SELECT count(*) FROM msgs_msg WHERE status = 'P' AND queued_on > $1 AND sent_on IS NULL`, now).Returns(3)

	assertdb.Query(t, db, `SELECT status, failed_reason FROM msgs_msg WHERE id = $1`, out4.ID()).Columns(map[string]interface{}{"status": "F", "failed_reason": "D"})
	assertdb.Query(t, db, `SELECT status, failed_reason FROM msgs_msg WHERE id = $1`, out5.ID()).Columns(map[string]interface{}{"status": "F", "failed_reason": "D"})
}

func TestGetMsgRepetitions(t *testing.T) {
	_, rt, db, rp := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetRedis)
	defer dates.SetNowSource(dates.DefaultNowSource)

	dates.SetNowSource(dates.NewFixedNowSource(time.Date(2021, 11, 18, 12, 13, 3, 234567, time.UTC)))

	oa := testdata.Org1.Load(rt)
	_, cathy := testdata.Cathy.Load(db, oa)

	msg1 := flows.NewMsgOut(testdata.Cathy.URN, nil, "foo", nil, nil, nil, flows.NilMsgTopic, flows.NilUnsendableReason)
	msg2 := flows.NewMsgOut(testdata.Cathy.URN, nil, "bar", nil, nil, nil, flows.NilMsgTopic, flows.NilUnsendableReason)

	assertRepetitions := func(m *flows.MsgOut, expected int) {
		count, err := models.GetMsgRepetitions(rp, cathy, m)
		require.NoError(t, err)
		assert.Equal(t, expected, count)
	}

	// keep counts up to 99
	for i := 0; i < 99; i++ {
		assertRepetitions(msg1, i+1)
	}
	assertredis.HGetAll(t, rp, "msg_repetitions:2021-11-18T12:15", map[string]string{"10000": "99:foo"})

	for i := 0; i < 50; i++ {
		assertRepetitions(msg1, 99)
	}
	assertredis.HGetAll(t, rp, "msg_repetitions:2021-11-18T12:15", map[string]string{"10000": "99:foo"})

	for i := 0; i < 19; i++ {
		assertRepetitions(msg2, i+1)
	}
	assertredis.HGetAll(t, rp, "msg_repetitions:2021-11-18T12:15", map[string]string{"10000": "19:bar"})

	for i := 0; i < 50; i++ {
		assertRepetitions(msg2, 20+i)
	}
	assertredis.HGetAll(t, rp, "msg_repetitions:2021-11-18T12:15", map[string]string{"10000": "69:bar"})
}

func TestNormalizeAttachment(t *testing.T) {
	_, rt, _, _ := testsuite.Get()

	rt.Config.AttachmentDomain = "foo.bar.com"
	defer func() { rt.Config.AttachmentDomain = "" }()

	tcs := []struct {
		raw        string
		normalized string
	}{
		{"geo:-2.90875,-79.0117686", "geo:-2.90875,-79.0117686"},
		{"image/jpeg:http://files.com/test.jpg", "image/jpeg:http://files.com/test.jpg"},
		{"image/jpeg:https://files.com/test.jpg", "image/jpeg:https://files.com/test.jpg"},
		{"image/jpeg:test.jpg", "image/jpeg:https://foo.bar.com/test.jpg"},
		{"image/jpeg:/test.jpg", "image/jpeg:https://foo.bar.com/test.jpg"},
	}

	for _, tc := range tcs {
		assert.Equal(t, tc.normalized, string(models.NormalizeAttachment(rt.Config, utils.Attachment(tc.raw))))
	}
}

func TestMarkMessages(t *testing.T) {
	ctx, _, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetAll)

	flowMsg1 := testdata.InsertOutgoingMsg(db, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "Hello", nil, models.MsgStatusQueued, false)
	msgs, err := models.GetMessagesByID(ctx, db, testdata.Org1.ID, models.DirectionOut, []models.MsgID{models.MsgID(flowMsg1.ID())})
	require.NoError(t, err)
	msg1 := msgs[0]

	flowMsg2 := testdata.InsertOutgoingMsg(db, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "Hola", nil, models.MsgStatusQueued, false)
	msgs, err = models.GetMessagesByID(ctx, db, testdata.Org1.ID, models.DirectionOut, []models.MsgID{models.MsgID(flowMsg2.ID())})
	require.NoError(t, err)
	msg2 := msgs[0]

	testdata.InsertOutgoingMsg(db, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "Howdy", nil, models.MsgStatusQueued, false)

	models.MarkMessagesPending(ctx, db, []*models.Msg{msg1, msg2})

	assertdb.Query(t, db, `SELECT count(*) FROM msgs_msg WHERE status = 'P'`).Returns(2)

	// try running on database with BIGINT message ids
	db.MustExec(`ALTER SEQUENCE "msgs_msg_id_seq" AS bigint;`)
	db.MustExec(`ALTER SEQUENCE "msgs_msg_id_seq" RESTART WITH 3000000000;`)

	flowMsg4 := testdata.InsertOutgoingMsg(db, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "Big messages!", nil, models.MsgStatusQueued, false)
	msgs, err = models.GetMessagesByID(ctx, db, testdata.Org1.ID, models.DirectionOut, []models.MsgID{models.MsgID(flowMsg4.ID())})
	require.NoError(t, err)
	msg4 := msgs[0]

	assert.Equal(t, flows.MsgID(3000000000), msg4.ID())

	err = models.MarkMessagesPending(ctx, db, []*models.Msg{msg4})
	assert.NoError(t, err)

	assertdb.Query(t, db, `SELECT count(*) FROM msgs_msg WHERE status = 'P'`).Returns(3)
	assertdb.Query(t, db, `SELECT count(*) FROM msgs_msg WHERE status = 'Q'`).Returns(1)

	err = models.MarkMessagesQueued(ctx, db, []*models.Msg{msg4})
	assert.NoError(t, err)

	assertdb.Query(t, db, `SELECT count(*) FROM msgs_msg WHERE status = 'P'`).Returns(2)
	assertdb.Query(t, db, `SELECT count(*) FROM msgs_msg WHERE status = 'Q'`).Returns(2)
}

func TestNonPersistentBroadcasts(t *testing.T) {
	ctx, rt, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetData)

	ticket := testdata.InsertOpenTicket(db, testdata.Org1, testdata.Bob, testdata.Mailgun, testdata.DefaultTopic, "", "", time.Now(), nil)
	modelTicket := ticket.Load(db)

	translations := map[envs.Language]*models.BroadcastTranslation{envs.Language("eng"): {Text: "Hi there"}}

	// create a broadcast which doesn't actually exist in the DB
	bcast := models.NewBroadcast(
		testdata.Org1.ID,
		models.NilBroadcastID,
		translations,
		models.TemplateStateUnevaluated,
		envs.Language("eng"),
		[]urns.URN{"tel:+593979012345"},
		[]models.ContactID{testdata.Alexandria.ID, testdata.Bob.ID, testdata.Cathy.ID},
		[]models.GroupID{testdata.DoctorsGroup.ID},
		ticket.ID,
		models.NilUserID,
	)

	assert.Equal(t, models.NilBroadcastID, bcast.ID())
	assert.Equal(t, testdata.Org1.ID, bcast.OrgID())
	assert.Equal(t, envs.Language("eng"), bcast.BaseLanguage())
	assert.Equal(t, translations, bcast.Translations())
	assert.Equal(t, models.TemplateStateUnevaluated, bcast.TemplateState())
	assert.Equal(t, ticket.ID, bcast.TicketID())
	assert.Equal(t, []urns.URN{"tel:+593979012345"}, bcast.URNs())
	assert.Equal(t, []models.ContactID{testdata.Alexandria.ID, testdata.Bob.ID, testdata.Cathy.ID}, bcast.ContactIDs())
	assert.Equal(t, []models.GroupID{testdata.DoctorsGroup.ID}, bcast.GroupIDs())

	batch := bcast.CreateBatch([]models.ContactID{testdata.Alexandria.ID, testdata.Bob.ID})

	assert.Equal(t, models.NilBroadcastID, batch.BroadcastID)
	assert.Equal(t, testdata.Org1.ID, batch.OrgID)
	assert.Equal(t, envs.Language("eng"), batch.BaseLanguage)
	assert.Equal(t, translations, batch.Translations)
	assert.Equal(t, models.TemplateStateUnevaluated, batch.TemplateState)
	assert.Equal(t, ticket.ID, batch.TicketID)
	assert.Equal(t, []models.ContactID{testdata.Alexandria.ID, testdata.Bob.ID}, batch.ContactIDs)

	oa, err := models.GetOrgAssets(ctx, rt, testdata.Org1.ID)
	require.NoError(t, err)

	msgs, err := batch.CreateMessages(ctx, rt, oa)
	require.NoError(t, err)

	assert.Equal(t, 2, len(msgs))

	assertdb.Query(t, db, `SELECT count(*) FROM msgs_msg WHERE direction = 'O' AND broadcast_id IS NULL AND text = 'Hi there'`).Returns(2)

	// test ticket was updated
	assertdb.Query(t, db, `SELECT count(*) FROM tickets_ticket WHERE id = $1 AND last_activity_on > $2`, ticket.ID, modelTicket.LastActivityOn()).Returns(1)
}

func TestNewOutgoingIVR(t *testing.T) {
	ctx, rt, db, _ := testsuite.Get()

	oa, err := models.GetOrgAssets(ctx, rt, testdata.Org1.ID)
	require.NoError(t, err)

	vonage := oa.ChannelByUUID(testdata.VonageChannel.UUID)
	conn, err := models.InsertIVRConnection(ctx, db, testdata.Org1.ID, testdata.VonageChannel.ID, models.NilStartID, testdata.Cathy.ID, testdata.Cathy.URNID, models.ConnectionDirectionOut, models.ConnectionStatusInProgress, "")
	require.NoError(t, err)

	createdOn := time.Date(2021, 7, 26, 12, 6, 30, 0, time.UTC)

	flowMsg := flows.NewIVRMsgOut(testdata.Cathy.URN, vonage.ChannelReference(), "Hello", "eng", "http://example.com/hi.mp3")
	dbMsg := models.NewOutgoingIVR(rt.Config, testdata.Org1.ID, conn, flowMsg, createdOn)

	assert.Equal(t, flowMsg.UUID(), dbMsg.UUID())
	assert.Equal(t, "Hello", dbMsg.Text())
	assert.Equal(t, []utils.Attachment{"audio:http://example.com/hi.mp3"}, dbMsg.Attachments())
	assert.Equal(t, createdOn, dbMsg.CreatedOn())
	assert.Equal(t, &createdOn, dbMsg.SentOn())

	err = models.InsertMessages(ctx, db, []*models.Msg{dbMsg})
	require.NoError(t, err)

	assertdb.Query(t, db, `SELECT text, created_on, sent_on FROM msgs_msg WHERE uuid = $1`, dbMsg.UUID()).Columns(map[string]interface{}{"text": "Hello", "created_on": createdOn, "sent_on": createdOn})
}

func insertTestSession(t *testing.T, ctx context.Context, rt *runtime.Runtime, org *testdata.Org, contact *testdata.Contact, flow *testdata.Flow) *models.Session {
	testdata.InsertWaitingSession(rt.DB, org, contact, models.FlowTypeMessaging, testdata.Favorites, models.NilConnectionID, time.Now(), time.Now(), false, nil)

	oa, err := models.GetOrgAssets(ctx, rt, testdata.Org1.ID)
	require.NoError(t, err)

	_, flowContact := contact.Load(rt.DB, oa)

	session, err := models.FindWaitingSessionForContact(ctx, rt.DB, rt.SessionStorage, oa, models.FlowTypeMessaging, flowContact)
	require.NoError(t, err)

	return session
}
