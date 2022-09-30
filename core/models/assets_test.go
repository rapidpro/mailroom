package models_test

import (
	"encoding/json"
	"testing"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/assets/static"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAssets(t *testing.T) {
	ctx, rt, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetData)

	// create new flow with same name as an existing flow
	testdata.InsertFlow(db, testdata.Org1, []byte(`{
		"uuid": "fd7d16dd-3a38-4351-aea6-7a80acb41dd9",
		"name": "Pick a Number"
	}`))

	oa, err := models.GetOrgAssets(ctx, rt, testdata.Org1.ID)
	require.NoError(t, err)

	flow, err := oa.FlowByUUID(testdata.Favorites.UUID) // from db
	assert.NoError(t, err)
	assert.Equal(t, "Favorites", flow.Name())

	flow, err = oa.FlowByUUID(testdata.Favorites.UUID) // from cache
	assert.NoError(t, err)
	assert.Equal(t, "Favorites", flow.Name())

	flow, err = oa.FlowByUUID(`c1b78e46-de99-4ae1-beb8-205801178c35`)
	assert.Equal(t, err, models.ErrNotFound)
	assert.Nil(t, flow)

	flow, err = oa.FlowByName("PICK A NUMBER") // from db
	assert.NoError(t, err)
	assert.Equal(t, assets.FlowUUID("fd7d16dd-3a38-4351-aea6-7a80acb41dd9"), flow.UUID()) // new flow as newer saved_on
	assert.Equal(t, "Pick a Number", flow.Name())

	flow, err = oa.FlowByName("pick a number") // from cache
	assert.NoError(t, err)
	assert.Equal(t, "Pick a Number", flow.Name())

	flow, err = oa.FlowByName(`not a flow`)
	assert.Equal(t, err, models.ErrNotFound)
	assert.Nil(t, flow)

	dbFlow, err := oa.FlowByID(testdata.IVRFlow.ID) // from db
	assert.NoError(t, err)
	assert.Equal(t, "IVR Flow", dbFlow.Name())

	dbFlow, err = oa.FlowByID(testdata.IVRFlow.ID) // from cache
	assert.NoError(t, err)
	assert.Equal(t, "IVR Flow", dbFlow.Name())

	dbFlow, err = oa.FlowByID(123456)
	assert.Equal(t, err, models.ErrNotFound)
	assert.Nil(t, dbFlow)
}

func TestCloneForSimulation(t *testing.T) {
	ctx, rt, _, _ := testsuite.Get()

	defer testsuite.Reset(0)

	oa, err := models.GetOrgAssets(ctx, rt, testdata.Org1.ID)
	require.NoError(t, err)

	newFavoritesDef := `{
		"uuid": "9de3663f-c5c5-4c92-9f45-ecbc09abcc85",
		"name": "Favorites",
		"nodes": []
	}`

	newDefs := map[assets.FlowUUID]json.RawMessage{
		testdata.Favorites.UUID: []byte(newFavoritesDef),
	}

	testChannels := []assets.Channel{
		static.NewChannel("d7be3965-4c76-4abd-af78-ebc0b84ab621", "Test Channel 1", "1234567890", []string{"tel"}, nil, nil),
		static.NewChannel("fd130d20-65f8-43fc-a3c5-a3fa4d1e4193", "Test Channel 2", "2345678901", []string{"tel"}, nil, nil),
	}

	clone, err := oa.CloneForSimulation(ctx, rt, newDefs, testChannels)
	require.NoError(t, err)

	// should get new definition
	flow, err := clone.FlowByUUID(testdata.Favorites.UUID)
	require.NoError(t, err)
	assert.Equal(t, newFavoritesDef, string(flow.Definition()))

	// test channels should be accesible to engine
	testChannel1 := clone.SessionAssets().Channels().Get("d7be3965-4c76-4abd-af78-ebc0b84ab621")
	assert.Equal(t, "Test Channel 1", testChannel1.Name())
	testChannel2 := clone.SessionAssets().Channels().Get("fd130d20-65f8-43fc-a3c5-a3fa4d1e4193")
	assert.Equal(t, "Test Channel 2", testChannel2.Name())

	// as well as the regular channels
	vonage := clone.SessionAssets().Channels().Get(testdata.VonageChannel.UUID)
	assert.Equal(t, "Vonage", vonage.Name())

	// original assets still has original flow definition
	flow, err = oa.FlowByUUID(testdata.Favorites.UUID)
	require.NoError(t, err)
	assert.Equal(t, "{\"_ui\": {\"nodes\": {\"10c9c241-777f-4010-a841-6e87abed8520\": {\"typ", string(flow.Definition())[:64])

	// and doesn't have the test channels
	testChannel1 = oa.SessionAssets().Channels().Get("d7be3965-4c76-4abd-af78-ebc0b84ab621")
	assert.Nil(t, testChannel1)

	// can't override definition for a non-existent flow
	_, err = oa.CloneForSimulation(ctx, rt, map[assets.FlowUUID]json.RawMessage{"a121f1af-7dfa-47af-9d22-9726372e2daa": []byte(newFavoritesDef)}, nil)
	assert.EqualError(t, err, "unable to find flow with UUID 'a121f1af-7dfa-47af-9d22-9726372e2daa': not found")
}
