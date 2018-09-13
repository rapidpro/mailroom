package models

import (
	"context"
	"testing"

	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/utils"
	"github.com/stretchr/testify/assert"
	null "gopkg.in/guregu/null.v3"
)

func TestMsgs(t *testing.T) {
	ctx := context.Background()
	db := Reset(t)

	orgID := OrgID(1)
	chanID := ChannelID(2)
	chanUUID := assets.ChannelUUID(utils.UUID("c534272e-817d-4a78-a70c-f21df34407f8"))

	tcs := []struct {
		ChannelUUID  assets.ChannelUUID
		ChannelID    ChannelID
		Text         string
		ContactID    flows.ContactID
		URN          urns.URN
		ContactURNID ContactURNID
		Attachments  flows.AttachmentList
		QuickReplies []string
		Metadata     null.String
		HasErr       bool
	}{
		{chanUUID, chanID, "missing urn id", flows.ContactID(42), urns.URN("tel:+250700000001"), ContactURNID(0),
			nil, nil, null.NewString("", false), true},
		{chanUUID, chanID, "test outgoing", flows.ContactID(42), urns.URN("tel:+250700000001?id=42"), ContactURNID(42),
			nil, []string{"yes", "no"}, null.NewString(`{"quick_replies":["yes","no"]}`, true), false},
		{chanUUID, chanID, "test outgoing", flows.ContactID(42), urns.URN("tel:+250700000001?id=42"), ContactURNID(42),
			flows.AttachmentList([]flows.Attachment{flows.Attachment("image/jpeg:https://dl-foo.com/image.jpg")}), nil, null.NewString("", false), false},
	}

	for _, tc := range tcs {
		tx, err := db.BeginTxx(ctx, nil)
		assert.NoError(t, err)

		flowMsg := flows.NewMsgOut(tc.URN, assets.NewChannelReference(tc.ChannelUUID, "Test Channel"), tc.Text, tc.Attachments, tc.QuickReplies)
		msg, err := CreateOutgoingMsg(ctx, tx, orgID, tc.ChannelID, tc.ContactID, flowMsg)
		if err == nil {
			assert.Equal(t, orgID, msg.OrgID)
			assert.Equal(t, tc.Text, msg.Text)
			assert.Equal(t, tc.ContactID, msg.ContactID)
			assert.Equal(t, tc.ChannelID, msg.ChannelID)
			assert.Equal(t, tc.ChannelUUID, msg.ChannelUUID)
			assert.Equal(t, tc.URN, msg.URN)
			assert.Equal(t, tc.ContactURNID, msg.ContactURNID)
			assert.Equal(t, tc.Metadata, msg.Metadata)
			assert.True(t, msg.TopUpID.Valid)
			assert.True(t, msg.ID > 0)
			tx.Commit()
		} else {
			if !tc.HasErr {
				assert.Fail(t, "unexpected error: %s", err.Error())
			}
			tx.Rollback()
		}
	}
}
