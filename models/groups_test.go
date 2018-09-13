package models

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGroups(t *testing.T) {
	ctx := context.Background()
	db := Reset(t)

	groups, err := loadGroups(ctx, db, 1)
	assert.NoError(t, err)

	tcs := []struct {
		ID    GroupID
		Name  string
		Query string
	}{
		{GroupID(40), "Districts (Dynamic)", `district = "Faskari" OR district = "Zuru" OR district = "Anka"`},
		{GroupID(33), "Doctors", ""},
	}

	assert.Equal(t, 10, len(groups))
	for i, tc := range tcs {
		group := groups[i].(*Group)
		assert.Equal(t, tc.ID, group.ID())
		assert.Equal(t, tc.Name, group.Name())
		assert.Equal(t, tc.Query, group.Query())
	}
}
