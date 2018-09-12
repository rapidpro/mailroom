package models

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLocations(t *testing.T) {
	ctx := context.Background()
	db := Reset(t)

	db.MustExec(`INSERT INTO locations_boundaryalias(is_active, created_on, modified_on, name, boundary_id, created_by_id, modified_by_id, org_id)
											  VALUES(TRUE, NOW(), NOW(), 'Soko', 2, 1, 1, 1);`)
	db.MustExec(`INSERT INTO locations_boundaryalias(is_active, created_on, modified_on, name, boundary_id, created_by_id, modified_by_id, org_id)
	                                          VALUES(TRUE, NOW(), NOW(), 'Sokoz', 2, 1, 1, 2);`)

	root, err := loadLocations(ctx, db, 1)
	assert.NoError(t, err)

	assert.Equal(t, "192787", root.OSMID())
	assert.Equal(t, 0, root.Level())
	assert.Equal(t, "Nigeria", root.Name())
	assert.Equal(t, []string(nil), root.Aliases())
	assert.Equal(t, 37, len(root.Children()))

	tcs := []struct {
		OSMID       string
		Level       int
		Name        string
		Aliases     []string
		NumChildren int
	}{
		{"3707368", 1, "Sokoto", []string{"Soko"}, 23},
		{"3706956", 1, "Zamfara", nil, 14},
	}

	states := root.Children()
	for i, tc := range tcs {
		assert.Equal(t, tc.OSMID, states[i].OSMID())
		assert.Equal(t, tc.Level, states[i].Level())
		assert.Equal(t, tc.Name, states[i].Name())
		assert.Equal(t, tc.Aliases, states[i].Aliases())
		assert.Equal(t, tc.NumChildren, len(states[i].Children()))
	}
}
