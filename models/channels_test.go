package models

import (
	"testing"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/stretchr/testify/assert"
)

func TestChannels(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()

	// add some tel specific config to channel 2
	db.MustExec(`UPDATE channels_channel SET config = '{"matching_prefixes": ["250", "251"], "allow_international": true}' WHERE id = $1`, NexmoChannelID)

	// make twitter channel have a parent of twilio channel
	db.MustExec(`UPDATE channels_channel SET parent_id = $1 WHERE id = $2`, TwilioChannelID, TwitterChannelID)

	channels, err := loadChannels(ctx, db, 1)
	assert.NoError(t, err)

	tcs := []struct {
		ID                 ChannelID
		UUID               assets.ChannelUUID
		Name               string
		Address            string
		Schemes            []string
		Roles              []assets.ChannelRole
		Prefixes           []string
		AllowInternational bool
		Parent             *assets.ChannelReference
	}{
		{
			TwilioChannelID,
			TwilioChannelUUID,
			"Twilio",
			"+13605551212",
			[]string{"tel"},
			[]assets.ChannelRole{"send", "receive", "call", "answer"},
			nil,
			false,
			nil,
		},
		{
			NexmoChannelID,
			NexmoChannelUUID,
			"Nexmo",
			"5789",
			[]string{"tel"},
			[]assets.ChannelRole{"send", "receive"},
			[]string{"250", "251"},
			true,
			nil,
		},
		{
			TwitterChannelID,
			TwitterChannelUUID,
			"Twitter",
			"ureport",
			[]string{"twitter"},
			[]assets.ChannelRole{"send", "receive"},
			nil,
			false,
			assets.NewChannelReference(TwilioChannelUUID, "Twilio"),
		},
	}

	assert.Equal(t, len(tcs), len(channels))
	for i, tc := range tcs {
		channel := channels[i].(*Channel)
		assert.Equal(t, tc.UUID, channel.UUID())
		assert.Equal(t, tc.ID, channel.ID())
		assert.Equal(t, tc.Name, channel.Name())
		assert.Equal(t, tc.Address, channel.Address())
		assert.Equal(t, tc.Roles, channel.Roles())
		assert.Equal(t, tc.Schemes, channel.Schemes())
		assert.Equal(t, tc.Prefixes, channel.MatchPrefixes())
		assert.Equal(t, tc.AllowInternational, channel.AllowInternational())
		assert.Equal(t, tc.Parent, channel.Parent())
	}
}
