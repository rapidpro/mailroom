package models

import (
	"context"
	"testing"

	"github.com/nyaruka/goflow/assets"
	"github.com/stretchr/testify/assert"
)

func TestChannels(t *testing.T) {
	ctx := context.Background()
	db := Reset(t)

	channels, err := loadChannels(ctx, db, 1)
	assert.NoError(t, err)

	tcs := []struct {
		ID      ChannelID
		Name    string
		Address string
		Schemes []string
		Roles   []assets.ChannelRole
	}{
		{ChannelID(1), "Android", "1234", []string{"tel"}, []assets.ChannelRole{"send", "receive"}},
		{ChannelID(2), "Nexmo", "2345", []string{"tel"}, []assets.ChannelRole{"send", "receive"}},
		{ChannelID(3), "Twitter", "my_handle", []string{"twitter"}, []assets.ChannelRole{"send", "receive"}},
	}

	assert.Equal(t, len(tcs), len(channels))
	for i, tc := range tcs {
		channel := channels[i].(*Channel)
		assert.Equal(t, tc.ID, channel.ID())
		assert.Equal(t, tc.Name, channel.Name())
		assert.Equal(t, tc.Address, channel.Address())
		assert.Equal(t, tc.Roles, channel.Roles())
		assert.Equal(t, tc.Schemes, channel.Schemes())
	}
}
