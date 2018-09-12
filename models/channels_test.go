package models

import (
	"context"
	"testing"

	"github.com/nyaruka/goflow/flows"
	"github.com/stretchr/testify/assert"
)

func TestChannels(t *testing.T) {
	ctx := context.Background()
	db := Reset(t)

	channels, err := loadChannels(ctx, db, 1)
	assert.NoError(t, err)

	tcs := []struct {
		ID      flows.ChannelID
		Name    string
		Address string
		Schemes []string
		Roles   []string
	}{
		{flows.ChannelID(1), "Android", "1234", []string{"tel"}, []string{"send", "receive"}},
		{flows.ChannelID(2), "Nexmo", "2345", []string{"tel"}, []string{"send", "receive"}},
		{flows.ChannelID(3), "Twitter", "my_handle", []string{"twitter"}, []string{"send", "receive"}},
	}

	assert.Equal(t, len(tcs), len(channels))
	for i, tc := range tcs {
		assert.Equal(t, tc.ID, channels[i].ID())
		assert.Equal(t, tc.Name, channels[i].Name())
		assert.Equal(t, tc.Address, channels[i].Address())
		assert.Equal(t, tc.Roles, channels[i].Roles())
		assert.Equal(t, tc.Schemes, channels[i].Schemes())
	}
}
