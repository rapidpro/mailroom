package models_test

import (
	"testing"

	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadUsers(t *testing.T) {
	ctx, rt, _, _ := testsuite.Get()

	oa, err := models.GetOrgAssetsWithRefresh(ctx, rt, testdata.Org1.ID, models.RefreshUsers)
	require.NoError(t, err)

	users, err := oa.Users()
	require.NoError(t, err)

	expectedUsers := []struct {
		id    models.UserID
		email string
		name  string
		role  models.UserRole
	}{
		{testdata.Admin.ID, testdata.Admin.Email, "Andy Admin", models.UserRoleAdministrator},
		{testdata.Agent.ID, testdata.Agent.Email, "Ann D'Agent", models.UserRoleAgent},
		{testdata.Editor.ID, testdata.Editor.Email, "Ed McEditor", models.UserRoleEditor},
		{testdata.Surveyor.ID, testdata.Surveyor.Email, "Steve Surveys", models.UserRoleSurveyor},
		{testdata.Viewer.ID, testdata.Viewer.Email, "Veronica Views", models.UserRoleViewer},
	}

	require.Equal(t, len(expectedUsers), len(users))

	for i, expected := range expectedUsers {
		assetUser := users[i]
		assert.Equal(t, expected.email, assetUser.Email())
		assert.Equal(t, expected.name, assetUser.Name())

		modelUser := assetUser.(*models.User)
		assert.Equal(t, expected.id, modelUser.ID())
		assert.Equal(t, expected.email, modelUser.Email())
		assert.Equal(t, expected.role, modelUser.Role())

		assert.Equal(t, modelUser, oa.UserByID(expected.id))
		assert.Equal(t, modelUser, oa.UserByEmail(expected.email))
	}
}
