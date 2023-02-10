package models_test

import (
	"testing"

	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadGlobals(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer func() {
		rt.DB.MustExec(`UPDATE globals_global SET value = 'Nyaruka' WHERE org_id = $1 AND key = $2`, testdata.Org1.ID, "org_name")
	}()

	// set one of our global values to empty
	rt.DB.MustExec(`UPDATE globals_global SET value = '' WHERE org_id = $1 AND key = $2`, testdata.Org1.ID, "org_name")

	oa, err := models.GetOrgAssetsWithRefresh(ctx, rt, testdata.Org1.ID, models.RefreshGlobals)
	require.NoError(t, err)

	globals, err := oa.Globals()
	require.NoError(t, err)

	assert.Equal(t, 2, len(globals))
	assert.Equal(t, "access_token", globals[0].Key())
	assert.Equal(t, "Access Token", globals[0].Name())
	assert.Equal(t, "A213CD78", globals[0].Value())
	assert.Equal(t, "org_name", globals[1].Key())
	assert.Equal(t, "Org Name", globals[1].Name())
	assert.Equal(t, "", globals[1].Value())
}
