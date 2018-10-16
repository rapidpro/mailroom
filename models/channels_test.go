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

	// add some prefixes to channel 2
	db.MustExec(`UPDATE channels_channel SET config = '{"matching_prefixes": ["250", "251"]}' WHERE id = 2`)

	// make channel 3 have a parent of channel 1
	db.MustExec(`UPDATE channels_channel SET parent_id = 1 WHERE id = 3`)

	channels, err := loadChannels(ctx, db, 1)
	assert.NoError(t, err)

	tcs := []struct {
		ID       ChannelID
		UUID     assets.ChannelUUID
		Name     string
		Address  string
		Schemes  []string
		Roles    []assets.ChannelRole
		Prefixes []string
		Parent   *assets.ChannelReference
	}{
		{ChannelID(1), assets.ChannelUUID("ac4c718a-db3f-4d8a-ae43-321f1a5bd44a"), "Android", "1234",
			[]string{"tel"}, []assets.ChannelRole{"send", "receive"}, nil, nil},
		{ChannelID(2), assets.ChannelUUID("c534272e-817d-4a78-a70c-f21df34407f8"), "Nexmo", "2345",
			[]string{"tel"}, []assets.ChannelRole{"send", "receive"}, []string{"250", "251"}, nil},
		{ChannelID(3), assets.ChannelUUID("0b10b271-a4ec-480f-abed-b4a197490590"), "Twitter", "my_handle", []string{"twitter"}, []assets.ChannelRole{"send", "receive"}, nil,
			assets.NewChannelReference(assets.ChannelUUID("ac4c718a-db3f-4d8a-ae43-321f1a5bd44a"), "Android")},
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
		assert.Equal(t, tc.Parent, channel.Parent())
	}

}
