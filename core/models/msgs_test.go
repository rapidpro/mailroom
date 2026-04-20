package models_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/gocommon/dbutil/assertdb"
	"github.com/nyaruka/gocommon/i18n"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/nyaruka/null/v3"
	"github.com/nyaruka/redisx/assertredis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewOutgoingFlowMsg(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetData)

	blake := testdata.InsertContact(rt, testdata.Org1, "79b94a23-6d13-43f4-95fe-c733ee457857", "Blake", i18n.NilLanguage, models.ContactStatusBlocked)
	blakeURNID := testdata.InsertContactURN(rt, testdata.Org1, blake, "tel:++250700000007", 1, nil)

	tcs := []struct {
		Channel      *testdata.Channel
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
		ExpectedMetadata     map[string]any
		ExpectedMsgCount     int
		ExpectedPriority     bool
	}{
		{
			Channel:              testdata.TwilioChannel,
			Text:                 "missing urn id",
			Contact:              testdata.Cathy,
			URN:                  urns.URN("tel:+250700000001"),
			URNID:                models.URNID(0),
			Flow:                 testdata.Favorites,
			ResponseTo:           models.MsgID(123425),
			ExpectedStatus:       models.MsgStatusQueued,
			ExpectedFailedReason: models.NilMsgFailedReason,
			ExpectedMetadata:     map[string]any{},
			ExpectedMsgCount:     1,
			ExpectedPriority:     true,
		},
		{
			Channel:              testdata.TwilioChannel,
			Text:                 "test outgoing",
			Contact:              testdata.Cathy,
			URN:                  urns.URN(fmt.Sprintf("tel:+250700000001?id=%d", testdata.Cathy.URNID)),
			URNID:                testdata.Cathy.URNID,
			QuickReplies:         []string{"yes", "no"},
			Topic:                flows.MsgTopicPurchase,
			Flow:                 testdata.SingleMessage,
			ExpectedStatus:       models.MsgStatusQueued,
			ExpectedFailedReason: models.NilMsgFailedReason,
			ExpectedMetadata:     map[string]any{"topic": "purchase"},
			ExpectedMsgCount:     1,
			ExpectedPriority:     false,
		},
		{
			Channel:              testdata.TwilioChannel,
			Text:                 "test outgoing",
			Contact:              testdata.Cathy,
			URN:                  urns.URN(fmt.Sprintf("tel:+250700000001?id=%d", testdata.Cathy.URNID)),
			URNID:                testdata.Cathy.URNID,
			Attachments:          []utils.Attachment{utils.Attachment("image/jpeg:https://dl-foo.com/image.jpg")},
			Flow:                 testdata.Favorites,
			ExpectedStatus:       models.MsgStatusQueued,
			ExpectedFailedReason: models.NilMsgFailedReason,
			ExpectedMetadata:     map[string]any{},
			ExpectedMsgCount:     2,
			ExpectedPriority:     false,
		},
		{
			Channel:              testdata.TwilioChannel,
			Text:                 "suspended org",
			Contact:              testdata.Cathy,
			URN:                  urns.URN(fmt.Sprintf("tel:+250700000001?id=%d", testdata.Cathy.URNID)),
			URNID:                testdata.Cathy.URNID,
			Flow:                 testdata.Favorites,
			SuspendedOrg:         true,
			ExpectedStatus:       models.MsgStatusFailed,
			ExpectedFailedReason: models.MsgFailedSuspended,
			ExpectedMetadata:     map[string]any{},
			ExpectedMsgCount:     1,
			ExpectedPriority:     false,
		},
		{
			Channel:              nil,
			Text:                 "no destination",
			Contact:              testdata.Cathy,
			URN:                  urns.NilURN,
			URNID:                models.URNID(0),
			Unsendable:           flows.UnsendableReasonNoDestination,
			Flow:                 testdata.Favorites,
			ExpectedStatus:       models.MsgStatusFailed,
			ExpectedFailedReason: models.MsgFailedNoDestination,
			ExpectedMetadata:     map[string]any{},
			ExpectedMsgCount:     1,
			ExpectedPriority:     false,
		},
		{
			Channel:              testdata.TwilioChannel,
			Text:                 "blocked contact",
			Contact:              blake,
			URN:                  urns.URN(fmt.Sprintf("tel:+250700000007?id=%d", blakeURNID)),
			URNID:                blakeURNID,
			Unsendable:           flows.UnsendableReasonContactStatus,
			Flow:                 testdata.Favorites,
			ExpectedStatus:       models.MsgStatusFailed,
			ExpectedFailedReason: models.MsgFailedContact,
			ExpectedMetadata:     map[string]any{},
			ExpectedMsgCount:     1,
			ExpectedPriority:     false,
		},
	}

	now := time.Now()

	for _, tc := range tcs {
		desc := fmt.Sprintf("text='%s'", tc.Text)
		rt.DB.MustExec(`UPDATE orgs_org SET is_suspended = $1 WHERE id = $2`, tc.SuspendedOrg, testdata.Org1.ID)

		oa, err := models.GetOrgAssetsWithRefresh(ctx, rt, testdata.Org1.ID, models.RefreshOrg)
		require.NoError(t, err)

		var ch *models.Channel
		var chRef *assets.ChannelReference
		expectedChannelID := models.NilChannelID
		if tc.Channel != nil {
			ch = oa.ChannelByUUID(tc.Channel.UUID)
			chRef = ch.Reference()
			expectedChannelID = ch.ID()
		}

		flow, _ := oa.FlowByID(tc.Flow.ID)

		session := insertTestSession(t, ctx, rt, testdata.Org1, tc.Contact, testdata.Favorites)
		if tc.ResponseTo != models.NilMsgID {
			session.SetIncomingMsg(tc.ResponseTo, null.NullString)
		}

		flowMsg := flows.NewMsgOut(tc.URN, chRef, tc.Text, tc.Attachments, tc.QuickReplies, nil, tc.Topic, i18n.NilLocale, tc.Unsendable)
		msg, err := models.NewOutgoingFlowMsg(rt, oa.Org(), ch, session, flow, flowMsg, nil, now)

		assert.NoError(t, err)

		expectedAttachments := tc.Attachments
		if expectedAttachments == nil {
			expectedAttachments = []utils.Attachment{}
		}

		err = models.InsertMessages(ctx, rt.DB, []*models.Msg{msg})
		assert.NoError(t, err)
		assert.Equal(t, oa.OrgID(), msg.OrgID())
		assert.Equal(t, tc.Text, msg.Text())
		assert.Equal(t, models.MsgTypeText, msg.Type())
		assert.Equal(t, expectedAttachments, msg.Attachments())
		assert.Equal(t, tc.QuickReplies, msg.QuickReplies())
		assert.Equal(t, tc.Contact.ID, msg.ContactID())
		assert.Equal(t, expectedChannelID, msg.ChannelID())
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
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM msgs_msg WHERE failed_reason IS NOT NULL`).Returns(3)

	// ensure org is unsuspended
	rt.DB.MustExec(`UPDATE orgs_org SET is_suspended = FALSE`)
	models.FlushCache()

	oa, err := models.GetOrgAssetsWithRefresh(ctx, rt, testdata.Org1.ID, models.RefreshOrg)
	require.NoError(t, err)
	channel := oa.ChannelByUUID(testdata.TwilioChannel.UUID)
	flow, _ := oa.FlowByID(testdata.Favorites.ID)
	session := insertTestSession(t, ctx, rt, testdata.Org1, testdata.Cathy, testdata.Favorites)

	// check that msg loop detection triggers after 20 repeats of the same text
	newOutgoing := func(text string) *models.Msg {
		flowMsg := flows.NewMsgOut(urns.URN(fmt.Sprintf("tel:+250700000001?id=%d", testdata.Cathy.URNID)), assets.NewChannelReference(testdata.TwilioChannel.UUID, "Twilio"), text, nil, nil, nil, flows.NilMsgTopic, i18n.NilLocale, flows.NilUnsendableReason)
		msg, err := models.NewOutgoingFlowMsg(rt, oa.Org(), channel, session, flow, flowMsg, nil, now)
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

func TestGetMessagesByID(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetData)

	msgIn1 := testdata.InsertIncomingMsg(rt, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "in 1", models.MsgStatusHandled)
	msgOut1 := testdata.InsertOutgoingMsg(rt, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "out 1", []utils.Attachment{"image/jpeg:hi.jpg"}, models.MsgStatusSent, false)
	msgOut2 := testdata.InsertOutgoingMsg(rt, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "out 2", nil, models.MsgStatusSent, false)
	msgOut3 := testdata.InsertOutgoingMsg(rt, testdata.Org2, testdata.Org2Channel, testdata.Org2Contact, "out 3", nil, models.MsgStatusSent, false)
	testdata.InsertOutgoingMsg(rt, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "hi 3", nil, models.MsgStatusSent, false)

	ids := []models.MsgID{msgIn1.ID, msgOut1.ID, msgOut2.ID, msgOut3.ID}

	msgs, err := models.GetMessagesByID(ctx, rt.DB, testdata.Org1.ID, models.DirectionOut, ids)

	// should only return the outgoing messages for this org
	require.NoError(t, err)
	assert.Equal(t, 2, len(msgs))
	assert.Equal(t, "out 1", msgs[0].Text())
	assert.Equal(t, []utils.Attachment{"image/jpeg:hi.jpg"}, msgs[0].Attachments())
	assert.Equal(t, "out 2", msgs[1].Text())

	msgs, err = models.GetMessagesByID(ctx, rt.DB, testdata.Org1.ID, models.DirectionIn, ids)

	// should only return the incoming message for this org
	require.NoError(t, err)
	assert.Equal(t, 1, len(msgs))
	assert.Equal(t, "in 1", msgs[0].Text())
}

func TestResendMessages(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetAll)

	oa, err := models.GetOrgAssets(ctx, rt, testdata.Org1.ID)
	require.NoError(t, err)

	out1 := testdata.InsertOutgoingMsg(rt, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "hi", nil, models.MsgStatusFailed, false)
	out2 := testdata.InsertOutgoingMsg(rt, testdata.Org1, testdata.TwilioChannel, testdata.Bob, "hi", nil, models.MsgStatusFailed, false)

	// failed message with no channel
	out3 := testdata.InsertOutgoingMsg(rt, testdata.Org1, nil, testdata.Cathy, "hi", nil, models.MsgStatusFailed, false)

	// failed message with no URN
	out4 := testdata.InsertOutgoingMsg(rt, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "hi", nil, models.MsgStatusFailed, false)
	rt.DB.MustExec(`UPDATE msgs_msg SET contact_urn_id = NULL, failed_reason = 'D' WHERE id = $1`, out4.ID)

	// failed message with URN which we no longer have a channel for
	out5 := testdata.InsertOutgoingMsg(rt, testdata.Org1, nil, testdata.George, "hi", nil, models.MsgStatusFailed, false)
	rt.DB.MustExec(`UPDATE msgs_msg SET failed_reason = 'E' WHERE id = $1`, out5.ID)
	rt.DB.MustExec(`UPDATE contacts_contacturn SET scheme = 'viber', path = '1234', identity = 'viber:1234' WHERE id = $1`, testdata.George.URNID)

	// other failed message not included in set to resend
	testdata.InsertOutgoingMsg(rt, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "hi", nil, models.MsgStatusFailed, false)

	// give Bob's URN an affinity for the Vonage channel
	rt.DB.MustExec(`UPDATE contacts_contacturn SET channel_id = $1 WHERE id = $2`, testdata.VonageChannel.ID, testdata.Bob.URNID)

	ids := []models.MsgID{out1.ID, out2.ID, out3.ID, out4.ID, out5.ID}
	msgs, err := models.GetMessagesByID(ctx, rt.DB, testdata.Org1.ID, models.DirectionOut, ids)
	require.NoError(t, err)

	now := dates.Now()

	// resend both msgs
	resent, err := models.ResendMessages(ctx, rt, oa, msgs)
	require.NoError(t, err)

	assert.Len(t, resent, 3) // only #1, #2 and #3 can be resent

	// both messages should now have a channel and be marked for resending
	assert.True(t, resent[0].IsResend)
	assert.Equal(t, testdata.TwilioChannel.ID, resent[0].ChannelID())
	assert.True(t, resent[1].IsResend)
	assert.Equal(t, testdata.VonageChannel.ID, resent[1].ChannelID()) // channel changed
	assert.True(t, resent[2].IsResend)
	assert.Equal(t, testdata.TwilioChannel.ID, resent[2].ChannelID()) // channel added

	assertdb.Query(t, rt.DB, `SELECT count(*) FROM msgs_msg WHERE status = 'Q' AND queued_on > $1 AND sent_on IS NULL`, now).Returns(3)

	assertdb.Query(t, rt.DB, `SELECT status, failed_reason FROM msgs_msg WHERE id = $1`, out4.ID).Columns(map[string]any{"status": "F", "failed_reason": "D"})
	assertdb.Query(t, rt.DB, `SELECT status, failed_reason FROM msgs_msg WHERE id = $1`, out5.ID).Columns(map[string]any{"status": "F", "failed_reason": "D"})
}

func TestFailMessages(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetData)

	testdata.InsertOutgoingMsg(rt, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "hi", nil, models.MsgStatusPending, false)
	testdata.InsertOutgoingMsg(rt, testdata.Org1, testdata.TwilioChannel, testdata.Bob, "hi", nil, models.MsgStatusErrored, false)
	out3 := testdata.InsertOutgoingMsg(rt, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "hi", nil, models.MsgStatusFailed, false)
	testdata.InsertOutgoingMsg(rt, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "hi", nil, models.MsgStatusQueued, false)
	testdata.InsertOutgoingMsg(rt, testdata.Org1, testdata.TwilioChannel, testdata.George, "hi", nil, models.MsgStatusQueued, false)

	now := dates.Now()

	// fail the msgs
	err := models.FailChannelMessages(ctx, rt.DB.DB, testdata.Org1.ID, testdata.TwilioChannel.ID, models.MsgFailedChannelRemoved)
	require.NoError(t, err)

	assertdb.Query(t, rt.DB, `SELECT count(*) FROM msgs_msg WHERE status = 'F' AND modified_on > $1`, now).Returns(4)
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM msgs_msg WHERE status = 'F' AND failed_reason = 'R' AND modified_on > $1`, now).Returns(4)
	assertdb.Query(t, rt.DB, `SELECT status, failed_reason FROM msgs_msg WHERE id = $1`, out3.ID).Columns(map[string]any{"status": "F", "failed_reason": nil})
}

func TestUpdateMessageDeletedBySender(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetData)

	in1 := testdata.InsertIncomingMsg(rt, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "hi", models.MsgStatusHandled)
	in1.Label(rt, testdata.ReportingLabel, testdata.TestingLabel)
	in2 := testdata.InsertIncomingMsg(rt, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "bye", models.MsgStatusHandled)
	in2.Label(rt, testdata.ReportingLabel, testdata.TestingLabel)
	out1 := testdata.InsertOutgoingMsg(rt, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "hi", nil, models.MsgStatusSent, false)

	err := models.UpdateMessageDeletedBySender(ctx, rt.DB.DB, testdata.Org1.ID, in1.ID)
	assert.NoError(t, err)

	assertdb.Query(t, rt.DB, `SELECT visibility, text FROM msgs_msg WHERE id = $1`, in1.ID).Columns(map[string]any{"visibility": "X", "text": ""})
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM msgs_msg_labels WHERE msg_id = $1`, in1.ID).Returns(0)
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM msgs_msg_labels WHERE msg_id = $1`, in2.ID).Returns(2) // unchanged

	// trying to delete an outgoing message is a noop
	err = models.UpdateMessageDeletedBySender(ctx, rt.DB.DB, testdata.Org1.ID, out1.ID)
	assert.NoError(t, err)

	assertdb.Query(t, rt.DB, `SELECT visibility, text FROM msgs_msg WHERE id = $1`, out1.ID).Columns(map[string]any{"visibility": "V", "text": "hi"})
}

func TestGetMsgRepetitions(t *testing.T) {
	_, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetRedis)
	defer dates.SetNowSource(dates.DefaultNowSource)

	dates.SetNowSource(dates.NewFixedNowSource(time.Date(2021, 11, 18, 12, 13, 3, 234567, time.UTC)))

	oa := testdata.Org1.Load(rt)
	_, cathy, _ := testdata.Cathy.Load(rt, oa)
	_, george, _ := testdata.George.Load(rt, oa)

	msg1 := flows.NewMsgOut(testdata.Cathy.URN, nil, "foo", nil, nil, nil, flows.NilMsgTopic, i18n.NilLocale, flows.NilUnsendableReason)
	msg2 := flows.NewMsgOut(testdata.Cathy.URN, nil, "FOO", nil, nil, nil, flows.NilMsgTopic, i18n.NilLocale, flows.NilUnsendableReason)
	msg3 := flows.NewMsgOut(testdata.Cathy.URN, nil, "bar", nil, nil, nil, flows.NilMsgTopic, i18n.NilLocale, flows.NilUnsendableReason)
	msg4 := flows.NewMsgOut(testdata.George.URN, nil, "foo", nil, nil, nil, flows.NilMsgTopic, i18n.NilLocale, flows.NilUnsendableReason)

	assertRepetitions := func(contact *flows.Contact, m *flows.MsgOut, expected int) {
		count, err := models.GetMsgRepetitions(rt.RP, contact, m)
		require.NoError(t, err)
		assert.Equal(t, expected, count)
	}

	for i := 0; i < 20; i++ {
		assertRepetitions(cathy, msg1, i+1)
	}
	for i := 0; i < 10; i++ {
		assertRepetitions(cathy, msg2, i+21)
	}
	for i := 0; i < 5; i++ {
		assertRepetitions(cathy, msg3, i+1)
	}
	for i := 0; i < 5; i++ {
		assertRepetitions(george, msg4, i+1)
	}
	assertredis.HGetAll(t, rt.RP, "msg_repetitions:2021-11-18T12:15", map[string]string{"10000|foo": "30", "10000|bar": "5", "10002|foo": "5"})
}

func TestNormalizeAttachment(t *testing.T) {
	_, rt := testsuite.Runtime()

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
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetData)

	out1 := testdata.InsertOutgoingMsg(rt, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "Hello", nil, models.MsgStatusQueued, false)
	msgs, err := models.GetMessagesByID(ctx, rt.DB, testdata.Org1.ID, models.DirectionOut, []models.MsgID{out1.ID})
	require.NoError(t, err)
	msg1 := msgs[0]

	out2 := testdata.InsertOutgoingMsg(rt, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "Hola", nil, models.MsgStatusQueued, false)
	msgs, err = models.GetMessagesByID(ctx, rt.DB, testdata.Org1.ID, models.DirectionOut, []models.MsgID{out2.ID})
	require.NoError(t, err)
	msg2 := msgs[0]

	testdata.InsertOutgoingMsg(rt, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "Howdy", nil, models.MsgStatusQueued, false)

	models.MarkMessagesForRequeuing(ctx, rt.DB, []*models.Msg{msg1, msg2})

	assertdb.Query(t, rt.DB, `SELECT count(*) FROM msgs_msg WHERE status = 'I'`).Returns(2)

	// try running on database with BIGINT message ids
	rt.DB.MustExec(`ALTER SEQUENCE "msgs_msg_id_seq" AS bigint;`)
	rt.DB.MustExec(`ALTER SEQUENCE "msgs_msg_id_seq" RESTART WITH 3000000000;`)

	out4 := testdata.InsertOutgoingMsg(rt, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "Big messages!", nil, models.MsgStatusQueued, false)
	msgs, err = models.GetMessagesByID(ctx, rt.DB, testdata.Org1.ID, models.DirectionOut, []models.MsgID{out4.ID})
	require.NoError(t, err)
	msg4 := msgs[0]

	assert.Equal(t, flows.MsgID(3000000000), msg4.ID())

	err = models.MarkMessagesForRequeuing(ctx, rt.DB, []*models.Msg{msg4})
	assert.NoError(t, err)

	assertdb.Query(t, rt.DB, `SELECT count(*) FROM msgs_msg WHERE status = 'I'`).Returns(3)
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM msgs_msg WHERE status = 'Q'`).Returns(1)

	err = models.MarkMessagesQueued(ctx, rt.DB, []*models.Msg{msg4})
	assert.NoError(t, err)

	assertdb.Query(t, rt.DB, `SELECT count(*) FROM msgs_msg WHERE status = 'I'`).Returns(2)
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM msgs_msg WHERE status = 'Q'`).Returns(2)
}

func TestNewOutgoingIVR(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	oa, err := models.GetOrgAssets(ctx, rt, testdata.Org1.ID)
	require.NoError(t, err)

	vonage := oa.ChannelByUUID(testdata.VonageChannel.UUID)
	conn, err := models.InsertCall(ctx, rt.DB, testdata.Org1.ID, testdata.VonageChannel.ID, models.NilStartID, testdata.Cathy.ID, testdata.Cathy.URNID, models.CallDirectionOut, models.CallStatusInProgress, "")
	require.NoError(t, err)

	createdOn := time.Date(2021, 7, 26, 12, 6, 30, 0, time.UTC)

	flowMsg := flows.NewIVRMsgOut(testdata.Cathy.URN, vonage.Reference(), "Hello", "http://example.com/hi.mp3", "eng-US")
	dbMsg := models.NewOutgoingIVR(rt.Config, testdata.Org1.ID, conn, flowMsg, createdOn)

	assert.Equal(t, flowMsg.UUID(), dbMsg.UUID())
	assert.Equal(t, models.MsgTypeVoice, dbMsg.Type())
	assert.Equal(t, "Hello", dbMsg.Text())
	assert.Equal(t, []utils.Attachment{"audio:http://example.com/hi.mp3"}, dbMsg.Attachments())
	assert.Equal(t, i18n.Locale("eng-US"), dbMsg.Locale())
	assert.Equal(t, createdOn, dbMsg.CreatedOn())
	assert.Equal(t, &createdOn, dbMsg.SentOn())

	err = models.InsertMessages(ctx, rt.DB, []*models.Msg{dbMsg})
	require.NoError(t, err)

	assertdb.Query(t, rt.DB, `SELECT text, created_on, sent_on FROM msgs_msg WHERE uuid = $1`, dbMsg.UUID()).Columns(map[string]any{"text": "Hello", "created_on": createdOn, "sent_on": createdOn})
}

func TestNewMsgOut(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	oa, err := models.GetOrgAssets(ctx, rt, testdata.Org1.ID)
	require.NoError(t, err)

	_, cathy, _ := testdata.Cathy.Load(rt, oa)

	out, ch := models.NewMsgOut(oa, cathy, "hello", nil, nil, `eng-US`)
	assert.Equal(t, "hello", out.Text())
	assert.Equal(t, urns.URN("tel:+16055741111?id=10000"), out.URN())
	assert.Equal(t, assets.NewChannelReference("74729f45-7f29-4868-9dc4-90e491e3c7d8", "Twilio"), out.Channel())
	assert.Equal(t, i18n.Locale(`eng-US`), out.Locale())
	assert.Equal(t, "Twilio", ch.Name())

	cathy.SetStatus(flows.ContactStatusBlocked)

	out, ch = models.NewMsgOut(oa, cathy, "hello", nil, nil, `eng-US`)
	assert.Equal(t, urns.URN("tel:+16055741111?id=10000"), out.URN())
	assert.Equal(t, assets.NewChannelReference("74729f45-7f29-4868-9dc4-90e491e3c7d8", "Twilio"), out.Channel())
	assert.Equal(t, "Twilio", ch.Name())
	assert.Equal(t, flows.UnsendableReasonContactStatus, out.UnsendableReason())

	cathy.SetStatus(flows.ContactStatusActive)
	cathy.ClearURNs()

	out, ch = models.NewMsgOut(oa, cathy, "hello", nil, nil, `eng-US`)
	assert.Equal(t, urns.NilURN, out.URN())
	assert.Nil(t, out.Channel())
	assert.Nil(t, ch)
	assert.Equal(t, flows.UnsendableReasonNoDestination, out.UnsendableReason())
}

func insertTestSession(t *testing.T, ctx context.Context, rt *runtime.Runtime, org *testdata.Org, contact *testdata.Contact, flow *testdata.Flow) *models.Session {
	testdata.InsertWaitingSession(rt, org, contact, models.FlowTypeMessaging, testdata.Favorites, models.NilCallID, time.Now(), time.Now(), false, nil)

	oa, err := models.GetOrgAssets(ctx, rt, testdata.Org1.ID)
	require.NoError(t, err)

	_, flowContact, _ := contact.Load(rt, oa)

	session, err := models.FindWaitingSessionForContact(ctx, rt.DB, rt.SessionStorage, oa, models.FlowTypeMessaging, flowContact)
	require.NoError(t, err)

	return session
}
