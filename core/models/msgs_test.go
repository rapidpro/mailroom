package models_test

import (
	"fmt"
	"github.com/greatnonprofits-nfp/goflow/flows/events"
	"github.com/nyaruka/gocommon/jsonx"
	"testing"
	"time"

	"github.com/greatnonprofits-nfp/goflow/assets"
	"github.com/greatnonprofits-nfp/goflow/flows"
	"github.com/greatnonprofits-nfp/goflow/utils"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/mailroom/config"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOutgoingMsgs(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()

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
			ContactID:        models.CathyID,
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
			ContactID:      models.CathyID,
			URN:            urns.URN(fmt.Sprintf("tel:+250700000001?id=%d", models.CathyURNID)),
			URNID:          models.CathyURNID,
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
			ContactID:        models.CathyID,
			URN:              urns.URN(fmt.Sprintf("tel:+250700000001?id=%d", models.CathyURNID)),
			URNID:            models.CathyURNID,
			Attachments:      []utils.Attachment{utils.Attachment("image/jpeg:https://dl-foo.com/image.jpg")},
			ExpectedStatus:   models.MsgStatusQueued,
			ExpectedMetadata: map[string]interface{}{},
			ExpectedMsgCount: 2,
		},
		{
			ChannelUUID:      "74729f45-7f29-4868-9dc4-90e491e3c7d8",
			Text:             "suspended org",
			ContactID:        models.CathyID,
			URN:              urns.URN(fmt.Sprintf("tel:+250700000001?id=%d", models.CathyURNID)),
			URNID:            models.CathyURNID,
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

		db.MustExec(`UPDATE orgs_org SET is_suspended = $1 WHERE id = $2`, tc.SuspendedOrg, models.Org1)

		oa, err := models.GetOrgAssetsWithRefresh(ctx, db, models.Org1, models.RefreshOrg)
		require.NoError(t, err)

		channel := oa.ChannelByUUID(tc.ChannelUUID)

		flowMsg := flows.NewMsgOut(tc.URN, assets.NewChannelReference(tc.ChannelUUID, "Test Channel"), tc.Text, tc.Attachments, tc.QuickReplies, nil, tc.Topic, "", flows.ShareableIconsConfig{})
		msg, err := models.NewOutgoingMsg(oa.Org(), channel, tc.ContactID, flowMsg, now)

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
	ctx := testsuite.CTX()
	db := testsuite.DB()
	msgIn := testdata.InsertIncomingMsg(t, db, models.Org1, models.CathyID, models.CathyURN, models.CathyURNID, "hi there")

	msgID, err := models.GetMessageIDFromUUID(ctx, db, msgIn.UUID())

	require.NoError(t, err)
	assert.Equal(t, models.MsgID(msgIn.ID()), msgID)
}

func TestNormalizeAttachment(t *testing.T) {
	config.Mailroom.AttachmentDomain = "foo.bar.com"
	defer func() { config.Mailroom.AttachmentDomain = "" }()

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
		assert.Equal(t, tc.normalized, string(models.NormalizeAttachment(utils.Attachment(tc.raw))))
	}
}

func TestMarkMessages(t *testing.T) {
	ctx, db, _ := testsuite.Reset()
	defer testsuite.Reset()

	oa, err := models.GetOrgAssetsWithRefresh(ctx, db, models.Org1, models.RefreshOrg)
	require.NoError(t, err)

	channel := oa.ChannelByUUID(models.TwilioChannelUUID)

	insertMsg := func(text string) *models.Msg {
		urn := urns.URN(fmt.Sprintf("tel:+250700000001?id=%d", models.CathyURNID))
		flowMsg := flows.NewMsgOut(urn, channel.ChannelReference(), text, nil, nil, nil, flows.NilMsgTopic, "", flows.ShareableIconsConfig{Text: "hi there"})
		msg, err := models.NewOutgoingMsg(oa.Org(), channel, models.CathyID, flowMsg, time.Now())
		require.NoError(t, err)

		err = models.InsertMessages(ctx, db, []*models.Msg{msg})
		require.NoError(t, err)

		return msg
	}

	msg1 := insertMsg("Hello")
	msg2 := insertMsg("Hola")
	insertMsg("Howdy")

	models.MarkMessagesPending(ctx, db, []*models.Msg{msg1, msg2})

	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM msgs_msg WHERE status = 'P'`, nil, 2)

	// try running on database with BIGINT message ids
	db.MustExec(`ALTER TABLE "msgs_msg" ALTER COLUMN "id" TYPE bigint USING "id"::bigint;`)
	db.MustExec(`ALTER SEQUENCE "msgs_msg_id_seq" AS bigint;`)
	db.MustExec(`ALTER SEQUENCE "msgs_msg_id_seq" RESTART WITH 3000000000;`)
	db = testsuite.DB() // need new connection after changes

	msg4 := insertMsg("Big messages!")
	assert.Equal(t, flows.MsgID(3000000000), msg4.ID())

	err = models.MarkMessagesPending(ctx, db, []*models.Msg{msg4})
	assert.NoError(t, err)

	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM msgs_msg WHERE status = 'P'`, nil, 3)
}

func TestInsertChildBroadcast(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()
	defer testsuite.Reset()

	bEvent, err := getBroadcastEventFromJSON()
	assert.NoError(t, err)

	oa, err := models.GetOrgAssetsWithRefresh(ctx, db, models.Org1, models.RefreshFields|models.RefreshGroups)
	assert.NoError(t, err)

	broadcast, err := models.NewBroadcastFromEvent(ctx, db, oa, &bEvent)
	assert.NoError(t, err)

	cloned, err := models.InsertChildBroadcast(ctx, db, broadcast)
	assert.NoError(t, err)

	assert.Equal(t, broadcast.URNs(), cloned.URNs())
	assert.NotEqual(t, broadcast.BroadcastID(), cloned.BroadcastID())
}

func TestNewIncomingIVR(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()

	defer func() { testsuite.ResetDB() }()
	msgIn := testdata.InsertIncomingMsg(t, db, models.Org1, models.CathyID, models.CathyURN, models.CathyURNID, "hi there")
	conn, err := models.InsertIVRConnection(ctx, db, models.Org1, models.TwilioChannelID, models.NilStartID, models.CathyID, models.CathyURNID, models.ConnectionDirectionOut, models.ConnectionStatusPending, "")
	assert.NoError(t, err)
	ivrMsgIn := models.NewIncomingIVR(models.Org1, conn, msgIn, time.Now())

	assert.NotEqual(t, ivrMsgIn.ID(), msgIn.ID())
	assert.Equal(t, ivrMsgIn.UUID(), msgIn.UUID())
	assert.Equal(t, ivrMsgIn.Direction(), models.DirectionIn)
}

func TestNewNewOutgoingIVR(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()

	defer func() { testsuite.ResetDB() }()
	oa, err := models.GetOrgAssetsWithRefresh(ctx, db, models.Org1, models.RefreshOrg)
	require.NoError(t, err)

	channel := oa.ChannelByUUID(models.TwilioChannelUUID)
	msgOut := flows.NewMsgOut(models.CathyURN, channel.ChannelReference(), "test msg", nil, nil, nil, flows.NilMsgTopic, "", flows.ShareableIconsConfig{Text: "hi there"})


	conn, err := models.InsertIVRConnection(ctx, db, models.Org1, models.TwilioChannelID, models.NilStartID, models.CathyID, models.CathyURNID, models.ConnectionDirectionOut, models.ConnectionStatusPending, "")
	assert.NoError(t, err)

	ivrMsgOut, err := models.NewOutgoingIVR(models.Org1, conn, msgOut, time.Now())
	assert.NoError(t, err)

	assert.Equal(t, ivrMsgOut.UUID(), msgOut.UUID())
	assert.Equal(t, ivrMsgOut.Direction(), models.DirectionOut)
}

func TestCreateBroadcastMessages(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()
	rp := testsuite.RP()
	rc := rp.Get()

	defer func() {
		testsuite.ResetDB()
		rc.Close()
	}()
	oa, err := models.GetOrgAssetsWithRefresh(ctx, db, models.Org1, models.RefreshOrg)
	require.NoError(t, err)

	broadcast, err := getBroadcastFromJSON()
	assert.NoError(t, err)

	contactIDs := []models.ContactID{models.CathyID, models.BobID}
	batch := broadcast.CreateBatch(contactIDs)

	messages, err := models.CreateBroadcastMessages(ctx, db, rp, oa, batch)
	assert.NoError(t, err)
	for _, msg :=range messages {
		assert.Equal(t, msg.BroadcastID(), broadcast.BroadcastID())
		assert.Equal(t, msg.Text(), broadcast.Translations()["eng"].Text)
	}
}

func TestMarkBroadcastSent(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()
	defer func() { testsuite.ResetDB() }()
	msgCountSQL := `SELECT COUNT(*) FROM msgs_broadcast WHERE id = $1 AND status = 'S'`
	broadcast, err := getBroadcastFromJSON()
	assert.NoError(t, err)

	cloned, err := models.InsertChildBroadcast(ctx, db, &broadcast)
	assert.NoError(t, err)

	args := []interface{}{cloned.BroadcastID()}
	testsuite.AssertQueryCount(t, db, msgCountSQL, args, 0, " mismatch in expected count for query: %s", msgCountSQL)

	assert.NotEqual(t, broadcast.BroadcastID(), cloned.BroadcastID())
	err = models.MarkBroadcastSent(ctx, db, cloned.BroadcastID())
	assert.NoError(t, err)

	testsuite.AssertQueryCount(t, db, msgCountSQL, args, 1, " mismatch in expected count for query: %s", msgCountSQL)
}

func getBroadcastEventFromJSON() (events.BroadcastCreatedEvent, error) {
	bEventInst := events.BroadcastCreatedEvent{}
	bEvent := `
	{
		"step_uuid": "0dfc2fdb-fdad-4056-9dde-f9122ed51279",
	    "type": "broadcast_created",
	    "created_on": "2021-11-07T15:04:05Z",
	    "translations": {
	      "eng": {
	        "text": "hi, what's up"
	      },
	      "spa": {
	        "text": "Que pasa"
	      }
	    },
	    "base_language": "eng",
	    "urns": [],
	    "contacts": [{"uuid": "6393abc0-283d-4c9b-a1b3-641a035c34bf", "name": "Cathy"}]
	}
`
	err := jsonx.Unmarshal([]byte(bEvent), &bEventInst)

	return bEventInst, err
}

func getBroadcastFromJSON() (models.Broadcast, error) {
	broadcast := models.Broadcast{}
	broadcastJSON := `
	{
	   "translations": {
		  "eng":{
			 "text": "hi, what's up"
		  },
		  "spa":{
			 "text": "Que pasa"
		  }
	   },
	   "Text":{
		  "Map": null
	   },
	   "template_state": "evaluated",
	   "base_language": "eng",
	   "contact_ids": [
		  10000,
          10001
	   ],
	   "org_id": 1
	}
	`
	err := broadcast.UnmarshalJSON([]byte(broadcastJSON))
	return broadcast, err
}
