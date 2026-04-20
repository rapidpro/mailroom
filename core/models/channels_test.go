package models_test

import (
	"testing"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestChannels(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetAll)

	// add some tel specific config to channel 2
	rt.DB.MustExec(`UPDATE channels_channel SET config = '{"matching_prefixes": ["250", "251"], "allow_international": true}' WHERE id = $1`, testdata.VonageChannel.ID)

	oa, err := models.GetOrgAssetsWithRefresh(ctx, rt, 1, models.RefreshChannels)
	require.NoError(t, err)

	channels, err := oa.Channels()
	require.NoError(t, err)

	tcs := []struct {
		ID                 models.ChannelID
		UUID               assets.ChannelUUID
		Name               string
		Address            string
		Schemes            []string
		Roles              []assets.ChannelRole
		Features           []assets.ChannelFeature
		Prefixes           []string
		AllowInternational bool
	}{
		{
			testdata.TwilioChannel.ID,
			testdata.TwilioChannel.UUID,
			"Twilio",
			"+13605551212",
			[]string{"tel"},
			[]assets.ChannelRole{"send", "receive", "call", "answer"},
			[]assets.ChannelFeature{},
			nil,
			false,
		},
		{
			testdata.VonageChannel.ID,
			testdata.VonageChannel.UUID,
			"Vonage",
			"5789",
			[]string{"tel"},
			[]assets.ChannelRole{"send", "receive"},
			[]assets.ChannelFeature{},
			[]string{"250", "251"},
			true,
		},
		{
			testdata.FacebookChannel.ID,
			testdata.FacebookChannel.UUID,
			"Facebook",
			"12345",
			[]string{"facebook"},
			[]assets.ChannelRole{"send", "receive"},
			[]assets.ChannelFeature{"optins"},
			nil,
			false,
		},
	}

	assert.Equal(t, len(tcs), len(channels))
	for i, tc := range tcs {
		channel := channels[i].(*models.Channel)
		assert.Equal(t, tc.UUID, channel.UUID())
		assert.Equal(t, tc.ID, channel.ID())
		assert.Equal(t, tc.Name, channel.Name())
		assert.Equal(t, tc.Address, channel.Address())
		assert.Equal(t, tc.Roles, channel.Roles())
		assert.Equal(t, tc.Features, channel.Features())
		assert.Equal(t, tc.Schemes, channel.Schemes())
		assert.Equal(t, tc.Prefixes, channel.MatchPrefixes())
		assert.Equal(t, tc.AllowInternational, channel.AllowInternational())
	}
}
