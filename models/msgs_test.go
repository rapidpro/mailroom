package models

import (
	"testing"
	"time"

	sqlx_types "github.com/jmoiron/sqlx/types"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/stretchr/testify/assert"
)

func TestMsgs(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()

	orgID := OrgID(1)
	channels, err := loadChannels(ctx, db, orgID)
	assert.NoError(t, err)

	channel := channels[1].(*Channel)
	chanUUID := assets.ChannelUUID(utils.UUID("c534272e-817d-4a78-a70c-f21df34407f8"))

	tcs := []struct {
		ChannelUUID  assets.ChannelUUID
		Channel      *Channel
		Text         string
		ContactID    flows.ContactID
		URN          urns.URN
		ContactURNID URNID
		Attachments  flows.AttachmentList
		QuickReplies []string
		Metadata     sqlx_types.JSONText
		MsgCount     int
		HasErr       bool
	}{
		{chanUUID, channel, "missing urn id", flows.ContactID(42), urns.URN("tel:+250700000001"), URNID(0),
			nil, nil, sqlx_types.JSONText(nil), 1, true},
		{chanUUID, channel, "test outgoing", flows.ContactID(42), urns.URN("tel:+250700000001?id=42"), URNID(42),
			nil, []string{"yes", "no"}, sqlx_types.JSONText(`{"quick_replies":["yes","no"]}`), 1, false},
		{chanUUID, channel, "test outgoing", flows.ContactID(42), urns.URN("tel:+250700000001?id=42"), URNID(42),
			flows.AttachmentList([]flows.Attachment{flows.Attachment("image/jpeg:https://dl-foo.com/image.jpg")}), nil, sqlx_types.JSONText(nil), 2, false},
	}

	now := time.Now()
	time.Sleep(time.Millisecond * 10)

	for _, tc := range tcs {
		tx, err := db.BeginTxx(ctx, nil)
		assert.NoError(t, err)

		flowMsg := flows.NewMsgOut(tc.URN, assets.NewChannelReference(tc.ChannelUUID, "Test Channel"), tc.Text, tc.Attachments, tc.QuickReplies)
		msg, err := NewOutgoingMsg(orgID, tc.Channel, tc.ContactID, flowMsg, now)

		if err == nil {
			err = InsertMessages(ctx, tx, []*Msg{msg})
			assert.NoError(t, err)
			assert.Equal(t, orgID, msg.OrgID())
			assert.Equal(t, tc.Text, msg.Text())
			assert.Equal(t, tc.ContactID, msg.ContactID())
			assert.Equal(t, tc.Channel, msg.Channel())
			assert.Equal(t, tc.ChannelUUID, msg.ChannelUUID())
			assert.Equal(t, tc.URN, msg.URN())
			assert.Equal(t, tc.ContactURNID, msg.ContactURNID())
			assert.Equal(t, tc.Metadata, msg.Metadata())
			assert.Equal(t, tc.MsgCount, msg.MsgCount())
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
