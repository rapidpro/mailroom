package models

import (
	"context"
	"testing"

	"github.com/nyaruka/goflow/flows"
	"github.com/stretchr/testify/assert"
)

func TestGroups(t *testing.T) {
	ctx := context.Background()
	db := Reset(t)

	groups, err := loadGroups(ctx, db, 1)
	assert.NoError(t, err)

	tcs := []struct {
		ID    flows.GroupID
		Name  string
		Query string
	}{
		{flows.GroupID(40), "Districts (Dynamic)", `district = "Faskari" OR district = "Zuru" OR district = "Anka"`},
		{flows.GroupID(33), "Doctors", ""},
	}

	assert.Equal(t, 10, len(groups))
	for i, tc := range tcs {
		assert.Equal(t, tc.ID, groups[i].ID())
		assert.Equal(t, tc.Name, groups[i].Name())
		assert.Equal(t, tc.Query, groups[i].Query())
	}
}
