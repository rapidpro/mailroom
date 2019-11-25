package models

import (
	"fmt"
	"testing"
	"time"

	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/config"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/stretchr/testify/assert"
)

func TestMsgs(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()

	orgID := OrgID(1)
	channels, err := loadChannels(ctx, db, orgID)
	assert.NoError(t, err)

	channel := channels[0].(*Channel)
	chanUUID := channels[0].UUID()

	tcs := []struct {
		ChannelUUID      assets.ChannelUUID
		Channel          *Channel
		Text             string
		ContactID        ContactID
		URN              urns.URN
		ContactURNID     URNID
		Attachments      []utils.Attachment
		QuickReplies     []string
		Topic            flows.MsgTopic
		ExpectedMetadata map[string]interface{}
		ExpectedMsgCount int
		HasErr           bool
	}{
		{
			chanUUID, channel,
			"missing urn id",
			CathyID,
			urns.URN("tel:+250700000001"),
			URNID(0),
			nil,
			nil,
			flows.NilMsgTopic,
			map[string]interface{}{},
			1,
			true,
		},
		{
			chanUUID,
			channel,
			"test outgoing",
			CathyID,
			urns.URN(fmt.Sprintf("tel:+250700000001?id=%d", CathyURNID)),
			CathyURNID,
			nil,
			[]string{"yes", "no"},
			flows.MsgTopicPurchase,
			map[string]interface{}{
				"quick_replies": []string{"yes", "no"},
				"topic":         "purchase",
			},
			1,
			false,
		},
		{
			chanUUID,
			channel,
			"test outgoing",
			CathyID,
			urns.URN(fmt.Sprintf("tel:+250700000001?id=%d", CathyURNID)),
			CathyURNID,
			[]utils.Attachment{utils.Attachment("image/jpeg:https://dl-foo.com/image.jpg")},
			nil,
			flows.NilMsgTopic,
			map[string]interface{}{},
			2,
			false},
	}

	now := time.Now()
	time.Sleep(time.Millisecond * 10)

	for _, tc := range tcs {
		tx, err := db.BeginTxx(ctx, nil)
		assert.NoError(t, err)

		flowMsg := flows.NewMsgOut(tc.URN, assets.NewChannelReference(tc.ChannelUUID, "Test Channel"), tc.Text, tc.Attachments, tc.QuickReplies, nil, tc.Topic)
		msg, err := NewOutgoingMsg(orgID, tc.Channel, tc.ContactID, flowMsg, now)

		if err == nil {
			assert.False(t, tc.HasErr)
			err = InsertMessages(ctx, tx, []*Msg{msg})
			assert.NoError(t, err)
			assert.Equal(t, orgID, msg.OrgID())
			assert.Equal(t, tc.Text, msg.Text())
			assert.Equal(t, tc.ContactID, msg.ContactID())
			assert.Equal(t, tc.Channel, msg.Channel())
			assert.Equal(t, tc.ChannelUUID, msg.ChannelUUID())
			assert.Equal(t, tc.URN, msg.URN())
			if tc.ContactURNID != NilURNID {
				assert.Equal(t, tc.ContactURNID, *msg.ContactURNID())
			} else {
				assert.Nil(t, msg.ContactURNID())
			}
			assert.Equal(t, tc.ExpectedMetadata, msg.Metadata())
			assert.Equal(t, tc.ExpectedMsgCount, msg.MsgCount())
			assert.Equal(t, now, msg.CreatedOn())
			assert.True(t, msg.ID() > 0)
			assert.True(t, msg.QueuedOn().After(now))
			assert.True(t, msg.ModifiedOn().After(now))
		} else {
			if !tc.HasErr {
				assert.Fail(t, "unexpected error: %s", err.Error())
			}
		}
		tx.Rollback()
	}
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
		assert.Equal(t, tc.normalized, string(NormalizeAttachment(utils.Attachment(tc.raw))))
	}
}
