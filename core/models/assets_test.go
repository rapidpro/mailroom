package models_test

import (
	"encoding/json"
	"testing"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/assets/static/types"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCloneForSimulation(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()
	models.FlushCache()

	oa, err := models.GetOrgAssets(ctx, db, models.Org1)
	require.NoError(t, err)

	newFavoritesDef := `{
		"uuid": "9de3663f-c5c5-4c92-9f45-ecbc09abcc85",
		"name": "Favorites",
		"nodes": []
	}`

	newDefs := map[assets.FlowUUID]json.RawMessage{
		models.FavoritesFlowUUID: []byte(newFavoritesDef),
	}

	testChannels := []assets.Channel{
		types.NewChannel("d7be3965-4c76-4abd-af78-ebc0b84ab621", "Test Channel 1", "1234567890", []string{"tel"}, nil, nil),
		types.NewChannel("fd130d20-65f8-43fc-a3c5-a3fa4d1e4193", "Test Channel 2", "2345678901", []string{"tel"}, nil, nil),
	}

	clone, err := oa.CloneForSimulation(ctx, db, newDefs, testChannels)
	require.NoError(t, err)

	// should get new definition
	flow, err := clone.Flow(models.FavoritesFlowUUID)
	require.NoError(t, err)
	assert.Equal(t, newFavoritesDef, string(flow.Definition()))

	// test channels should be accesible to engine
	testChannel1 := clone.SessionAssets().Channels().Get("d7be3965-4c76-4abd-af78-ebc0b84ab621")
	assert.Equal(t, "Test Channel 1", testChannel1.Name())
	testChannel2 := clone.SessionAssets().Channels().Get("fd130d20-65f8-43fc-a3c5-a3fa4d1e4193")
	assert.Equal(t, "Test Channel 2", testChannel2.Name())

	// as well as the regular channels
	nexmo := clone.SessionAssets().Channels().Get(models.NexmoChannelUUID)
	assert.Equal(t, "Nexmo", nexmo.Name())

	// original assets still has original flow definition
	flow, err = oa.Flow(models.FavoritesFlowUUID)
	require.NoError(t, err)
	assert.Equal(t, "{\"_ui\": {\"nodes\": {\"10c9c241-777f-4010-a841-6e87abed8520\": {\"typ", string(flow.Definition())[:64])

	// and doesn't have the test channels
	testChannel1 = oa.SessionAssets().Channels().Get("d7be3965-4c76-4abd-af78-ebc0b84ab621")
	assert.Nil(t, testChannel1)

	// can't override definition for a non-existent flow
	oa, err = oa.CloneForSimulation(ctx, db, map[assets.FlowUUID]json.RawMessage{"a121f1af-7dfa-47af-9d22-9726372e2daa": []byte(newFavoritesDef)}, nil)
	assert.EqualError(t, err, "unable to find flow with UUID 'a121f1af-7dfa-47af-9d22-9726372e2daa': not found")
}
