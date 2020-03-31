package models

import (
	"testing"

	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/stretchr/testify/assert"
)

func TestLocations(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()

	db.MustExec(`INSERT INTO locations_boundaryalias(is_active, created_on, modified_on, name, boundary_id, created_by_id, modified_by_id, org_id)
											  VALUES(TRUE, NOW(), NOW(), 'Soko', 8148, 1, 1, 1);`)
	db.MustExec(`INSERT INTO locations_boundaryalias(is_active, created_on, modified_on, name, boundary_id, created_by_id, modified_by_id, org_id)
	                                          VALUES(TRUE, NOW(), NOW(), 'Sokoz', 8148, 1, 1, 2);`)

	root, err := loadLocations(ctx, db, 1)
	assert.NoError(t, err)

	locations := root[0].FindByName("Nigeria", 0, nil)

	assert.Equal(t, 1, len(locations))
	assert.Equal(t, "Nigeria", locations[0].Name())
	assert.Equal(t, []string(nil), locations[0].Aliases())
	assert.Equal(t, 37, len(locations[0].Children()))
	nigeria := locations[0]

	tcs := []struct {
		Name        string
		Level       utils.LocationLevel
		Aliases     []string
		NumChildren int
	}{
		{"Sokoto", 1, []string{"Soko"}, 23},
		{"Zamfara", 1, nil, 14},
	}

	for _, tc := range tcs {
		locations = root[0].FindByName(tc.Name, tc.Level, nigeria)
		assert.Equal(t, 1, len(locations))
		state := locations[0]

		assert.Equal(t, tc.Name, state.Name())
		assert.Equal(t, tc.Aliases, state.Aliases())
		assert.Equal(t, tc.NumChildren, len(state.Children()))
	}
}
