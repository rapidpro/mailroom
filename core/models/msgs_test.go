package models_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/utils"
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

		flowMsg := flows.NewMsgOut(tc.URN, assets.NewChannelReference(tc.ChannelUUID, "Test Channel"), tc.Text, tc.Attachments, tc.QuickReplies, nil, tc.Topic)
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
