package models_test

import (
	"testing"

	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSetFlowDefinition(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()
	models.FlushCache()

	oa, err := models.GetOrgAssets(ctx, db, models.Org1)
	require.NoError(t, err)

	// panic trying to set definition on non-cloned assets
	require.Panics(t, func() { oa.SetFlowDefinition(models.FavoritesFlowUUID, []byte(`{}`)) })

	oa, err = oa.Clone(ctx, db)
	require.NoError(t, err)

	// can't override definition for non-existing flow
	err = oa.SetFlowDefinition("a121f1af-7dfa-47af-9d22-9726372e2daa", []byte(`{}`))
	assert.EqualError(t, err, "unable to find flow with UUID 'a121f1af-7dfa-47af-9d22-9726372e2daa': not found")

	newDef := []byte(`{
		"uuid": "9de3663f-c5c5-4c92-9f45-ecbc09abcc85",
		"name": "Favorites",
		"nodes": []
	}`)
	err = oa.SetFlowDefinition(models.FavoritesFlowUUID, newDef)
	require.NoError(t, err)

	flow, err := oa.Flow(models.FavoritesFlowUUID)
	require.NoError(t, err)
	assert.Equal(t, string(newDef), string(flow.Definition()))
}
