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

	partners := &models.Team{testdata.Partners.ID, testdata.Partners.UUID, "Partners"}
	office := &models.Team{testdata.Office.ID, testdata.Office.UUID, "Office"}

	expectedUsers := []struct {
		id    models.UserID
		email string
		name  string
		role  models.UserRole
		team  *models.Team
	}{
		{id: testdata.Admin.ID, email: testdata.Admin.Email, name: "Andy Admin", role: models.UserRoleAdministrator, team: office},
		{id: testdata.Agent.ID, email: testdata.Agent.Email, name: "Ann D'Agent", role: models.UserRoleAgent, team: partners},
		{id: testdata.Editor.ID, email: testdata.Editor.Email, name: "Ed McEditor", role: models.UserRoleEditor, team: office},
		{id: testdata.Surveyor.ID, email: testdata.Surveyor.Email, name: "Steve Surveys", role: models.UserRoleSurveyor, team: nil},
		{id: testdata.Viewer.ID, email: testdata.Viewer.Email, name: "Veronica Views", role: models.UserRoleViewer, team: nil},
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
		assert.Equal(t, expected.team, modelUser.Team())

		assert.Equal(t, modelUser, oa.UserByID(expected.id))
		assert.Equal(t, modelUser, oa.UserByEmail(expected.email))
	}
}
