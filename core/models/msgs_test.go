package models_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOutgoingMsgs(t *testing.T) {
	ctx, rt, db, _ := testsuite.Get()

	tcs := []struct {
		ChannelUUID  assets.ChannelUUID
		Text         string
		ContactID    models.ContactID
		URN          urns.URN
		URNID        models.URNID
		Attachments  []utils.Attachment
		QuickReplies []string
		Topic        flows.MsgTopic
		SuspendedOrg bool

		ExpectedStatus   models.MsgStatus
		ExpectedMetadata map[string]interface{}
		ExpectedMsgCount int
		HasError         bool
	}{
		{
			ChannelUUID:      "74729f45-7f29-4868-9dc4-90e491e3c7d8",
			Text:             "missing urn id",
			ContactID:        testdata.Cathy.ID,
			URN:              urns.URN("tel:+250700000001"),
			URNID:            models.URNID(0),
			ExpectedStatus:   models.MsgStatusQueued,
			ExpectedMetadata: map[string]interface{}{},
			ExpectedMsgCount: 1,
			HasError:         true,
		},
		{
			ChannelUUID:    "74729f45-7f29-4868-9dc4-90e491e3c7d8",
			Text:           "test outgoing",
			ContactID:      testdata.Cathy.ID,
			URN:            urns.URN(fmt.Sprintf("tel:+250700000001?id=%d", testdata.Cathy.URNID)),
			URNID:          testdata.Cathy.URNID,
			QuickReplies:   []string{"yes", "no"},
			Topic:          flows.MsgTopicPurchase,
			ExpectedStatus: models.MsgStatusQueued,
			ExpectedMetadata: map[string]interface{}{
				"quick_replies": []string{"yes", "no"},
				"topic":         "purchase",
			},
			ExpectedMsgCount: 1,
		},
		{
			ChannelUUID:      "74729f45-7f29-4868-9dc4-90e491e3c7d8",
			Text:             "test outgoing",
			ContactID:        testdata.Cathy.ID,
			URN:              urns.URN(fmt.Sprintf("tel:+250700000001?id=%d", testdata.Cathy.URNID)),
			URNID:            testdata.Cathy.URNID,
			Attachments:      []utils.Attachment{utils.Attachment("image/jpeg:https://dl-foo.com/image.jpg")},
			ExpectedStatus:   models.MsgStatusQueued,
			ExpectedMetadata: map[string]interface{}{},
			ExpectedMsgCount: 2,
		},
		{
			ChannelUUID:      "74729f45-7f29-4868-9dc4-90e491e3c7d8",
			Text:             "suspended org",
			ContactID:        testdata.Cathy.ID,
			URN:              urns.URN(fmt.Sprintf("tel:+250700000001?id=%d", testdata.Cathy.URNID)),
			URNID:            testdata.Cathy.URNID,
			SuspendedOrg:     true,
			ExpectedStatus:   models.MsgStatusFailed,
			ExpectedMetadata: map[string]interface{}{},
			ExpectedMsgCount: 1,
		},
	}

	now := time.Now()

	for _, tc := range tcs {
		tx, err := db.BeginTxx(ctx, nil)
		require.NoError(t, err)

		db.MustExec(`UPDATE orgs_org SET is_suspended = $1 WHERE id = $2`, tc.SuspendedOrg, testdata.Org1.ID)

		oa, err := models.GetOrgAssetsWithRefresh(ctx, rt, testdata.Org1.ID, models.RefreshOrg)
		require.NoError(t, err)

		channel := oa.ChannelByUUID(tc.ChannelUUID)

		flowMsg := flows.NewMsgOut(tc.URN, assets.NewChannelReference(tc.ChannelUUID, "Test Channel"), tc.Text, tc.Attachments, tc.QuickReplies, nil, tc.Topic)
		msg, err := models.NewOutgoingMsg(rt.Config, oa.Org(), channel, tc.ContactID, flowMsg, now)

		if tc.HasError {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)

			err = models.InsertMessages(ctx, tx, []*models.Msg{msg})
			assert.NoError(t, err)
			assert.Equal(t, oa.OrgID(), msg.OrgID())
			assert.Equal(t, tc.Text, msg.Text())
			assert.Equal(t, tc.ContactID, msg.ContactID())
			assert.Equal(t, channel, msg.Channel())
			assert.Equal(t, tc.ChannelUUID, msg.ChannelUUID())
			assert.Equal(t, tc.URN, msg.URN())
			if tc.URNID != models.NilURNID {
				assert.Equal(t, tc.URNID, *msg.ContactURNID())
			} else {
				assert.Nil(t, msg.ContactURNID())
			}

			assert.Equal(t, tc.ExpectedStatus, msg.Status())
			assert.Equal(t, tc.ExpectedMetadata, msg.Metadata())
			assert.Equal(t, tc.ExpectedMsgCount, msg.MsgCount())
			assert.Equal(t, now, msg.CreatedOn())
			assert.True(t, msg.ID() > 0)
			assert.True(t, msg.QueuedOn().After(now))
			assert.True(t, msg.ModifiedOn().After(now))
		}

		tx.Rollback()
	}
}

func TestGetMessageIDFromUUID(t *testing.T) {
	ctx, _, db, _ := testsuite.Get()

	msgIn := testdata.InsertIncomingMsg(db, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "hi there", models.MsgStatusHandled)

	msgID, err := models.GetMessageIDFromUUID(ctx, db, msgIn.UUID())

	require.NoError(t, err)
	assert.Equal(t, models.MsgID(msgIn.ID()), msgID)
}

func TestLoadMessages(t *testing.T) {
	ctx, _, db, _ := testsuite.Get()

	msgIn1 := testdata.InsertIncomingMsg(db, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "in 1", models.MsgStatusHandled)
	msgOut1 := testdata.InsertOutgoingMsg(db, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "out 1", []utils.Attachment{"image/jpeg:hi.jpg"}, models.MsgStatusSent)
	msgOut2 := testdata.InsertOutgoingMsg(db, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "out 2", nil, models.MsgStatusSent)
	msgOut3 := testdata.InsertOutgoingMsg(db, testdata.Org2, testdata.Org2Channel, testdata.Org2Contact, "out 3", nil, models.MsgStatusSent)
	testdata.InsertOutgoingMsg(db, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "hi 3", nil, models.MsgStatusSent)

	ids := []models.MsgID{models.MsgID(msgIn1.ID()), models.MsgID(msgOut1.ID()), models.MsgID(msgOut2.ID()), models.MsgID(msgOut3.ID())}

	msgs, err := models.LoadMessages(ctx, db, testdata.Org1.ID, models.DirectionOut, ids)

	// should only return the outgoing messages for this org
	require.NoError(t, err)
	assert.Equal(t, 2, len(msgs))
	assert.Equal(t, "out 1", msgs[0].Text())
	assert.Equal(t, []utils.Attachment{"image/jpeg:hi.jpg"}, msgs[0].Attachments())
	assert.Equal(t, "out 2", msgs[1].Text())

	msgs, err = models.LoadMessages(ctx, db, testdata.Org1.ID, models.DirectionIn, ids)

	// should only return the incoming message for this org
	require.NoError(t, err)
	assert.Equal(t, 1, len(msgs))
	assert.Equal(t, "in 1", msgs[0].Text())
}

func TestResendMessages(t *testing.T) {
	ctx, rt, db, rp := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetAll)

	oa, err := models.GetOrgAssets(ctx, rt, testdata.Org1.ID)
	require.NoError(t, err)

	msgOut1 := testdata.InsertOutgoingMsg(db, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "out 1", nil, models.MsgStatusFailed)
	msgOut2 := testdata.InsertOutgoingMsg(db, testdata.Org1, testdata.TwilioChannel, testdata.Bob, "out 2", nil, models.MsgStatusFailed)
	testdata.InsertOutgoingMsg(db, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "out 3", nil, models.MsgStatusFailed)

	// give Bob's URN an affinity for the Vonage channel
	db.MustExec(`UPDATE contacts_contacturn SET channel_id = $1 WHERE id = $2`, testdata.VonageChannel.ID, testdata.Bob.URNID)

	msgs, err := models.LoadMessages(ctx, db, testdata.Org1.ID, models.DirectionOut, []models.MsgID{models.MsgID(msgOut1.ID()), models.MsgID(msgOut2.ID())})
	require.NoError(t, err)

	now := dates.Now()

	// resend both msgs
	err = models.ResendMessages(ctx, db, rp, oa, msgs)
	require.NoError(t, err)

	// both messages should now have a channel, a topup and be marked for resending
	assert.True(t, msgs[0].IsResend())
	assert.Equal(t, testdata.TwilioChannel.ID, msgs[0].ChannelID())
	assert.Equal(t, models.TopupID(1), msgs[0].TopupID())
	assert.True(t, msgs[1].IsResend())
	assert.Equal(t, testdata.VonageChannel.ID, msgs[1].ChannelID())
	assert.Equal(t, models.TopupID(1), msgs[1].TopupID())

	testsuite.AssertQuery(t, db, `SELECT count(*) FROM msgs_msg WHERE status = 'P' AND queued_on > $1 AND sent_on IS NULL`, now).Returns(2)
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
	ctx, rt, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetAll)

	oa, err := models.GetOrgAssetsWithRefresh(ctx, rt, testdata.Org1.ID, models.RefreshOrg)
	require.NoError(t, err)

	channel := oa.ChannelByUUID(testdata.TwilioChannel.UUID)

	insertMsg := func(text string) *models.Msg {
		urn := urns.URN(fmt.Sprintf("tel:+250700000001?id=%d", testdata.Cathy.URNID))
		flowMsg := flows.NewMsgOut(urn, channel.ChannelReference(), text, nil, nil, nil, flows.NilMsgTopic)
		msg, err := models.NewOutgoingMsg(rt.Config, oa.Org(), channel, testdata.Cathy.ID, flowMsg, time.Now())
		require.NoError(t, err)

		err = models.InsertMessages(ctx, db, []*models.Msg{msg})
		require.NoError(t, err)

		return msg
	}

	msg1 := insertMsg("Hello")
	msg2 := insertMsg("Hola")
	insertMsg("Howdy")

	models.MarkMessagesPending(ctx, db, []*models.Msg{msg1, msg2})

	testsuite.AssertQuery(t, db, `SELECT count(*) FROM msgs_msg WHERE status = 'P'`).Returns(2)

	// try running on database with BIGINT message ids
	db.MustExec(`ALTER SEQUENCE "msgs_msg_id_seq" AS bigint;`)
	db.MustExec(`ALTER SEQUENCE "msgs_msg_id_seq" RESTART WITH 3000000000;`)

	msg4 := insertMsg("Big messages!")
	assert.Equal(t, flows.MsgID(3000000000), msg4.ID())

	err = models.MarkMessagesPending(ctx, db, []*models.Msg{msg4})
	assert.NoError(t, err)

	testsuite.AssertQuery(t, db, `SELECT count(*) FROM msgs_msg WHERE status = 'P'`).Returns(3)
}

func TestNonPersistentBroadcasts(t *testing.T) {
	ctx, rt, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetData)

	ticket := testdata.InsertOpenTicket(db, testdata.Org1, testdata.Bob, testdata.Mailgun, testdata.DefaultTopic, "", "", nil)
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

	assert.Equal(t, models.NilBroadcastID, batch.BroadcastID())
	assert.Equal(t, testdata.Org1.ID, batch.OrgID())
	assert.Equal(t, envs.Language("eng"), batch.BaseLanguage())
	assert.Equal(t, translations, batch.Translations())
	assert.Equal(t, models.TemplateStateUnevaluated, batch.TemplateState())
	assert.Equal(t, ticket.ID, batch.TicketID())
	assert.Equal(t, []models.ContactID{testdata.Alexandria.ID, testdata.Bob.ID}, batch.ContactIDs())

	oa, err := models.GetOrgAssets(ctx, rt, testdata.Org1.ID)
	require.NoError(t, err)

	msgs, err := models.CreateBroadcastMessages(ctx, rt, oa, batch)
	require.NoError(t, err)

	assert.Equal(t, 2, len(msgs))

	testsuite.AssertQuery(t, db, `SELECT count(*) FROM msgs_msg WHERE direction = 'O' AND broadcast_id IS NULL AND text = 'Hi there'`).Returns(2)

	// test ticket was updated
	testsuite.AssertQuery(t, db, `SELECT count(*) FROM tickets_ticket WHERE id = $1 AND last_activity_on > $2`, ticket.ID, modelTicket.LastActivityOn()).Returns(1)
}

func TestNewOutgoingIVR(t *testing.T) {
	ctx, rt, db, _ := testsuite.Get()

	oa, err := models.GetOrgAssets(ctx, rt, testdata.Org1.ID)
	require.NoError(t, err)

	vonage := oa.ChannelByUUID(testdata.VonageChannel.UUID)
	conn, err := models.InsertIVRConnection(ctx, db, testdata.Org1.ID, testdata.VonageChannel.ID, models.NilStartID, testdata.Cathy.ID, testdata.Cathy.URNID, models.ConnectionDirectionOut, models.ConnectionStatusInProgress, "")
	require.NoError(t, err)

	createdOn := time.Date(2021, 7, 26, 12, 6, 30, 0, time.UTC)

	flowMsg := flows.NewMsgOut(testdata.Cathy.URN, vonage.ChannelReference(), "Hello", []utils.Attachment{"audio/mp3:http://example.com/hi.mp3"}, nil, nil, flows.NilMsgTopic)
	dbMsg := models.NewOutgoingIVR(rt.Config, testdata.Org1.ID, conn, flowMsg, createdOn)

	assert.Equal(t, flowMsg.UUID(), dbMsg.UUID())
	assert.Equal(t, "Hello", dbMsg.Text())
	assert.Equal(t, []utils.Attachment{"audio/mp3:http://example.com/hi.mp3"}, dbMsg.Attachments())
	assert.Equal(t, createdOn, dbMsg.CreatedOn())
	assert.Equal(t, &createdOn, dbMsg.SentOn())

	err = models.InsertMessages(ctx, db, []*models.Msg{dbMsg})
	require.NoError(t, err)

	testsuite.AssertQuery(t, db, `SELECT text, created_on, sent_on FROM msgs_msg WHERE uuid = $1`, dbMsg.UUID()).Columns(map[string]interface{}{"text": "Hello", "created_on": createdOn, "sent_on": createdOn})
}
