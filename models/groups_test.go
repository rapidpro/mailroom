package models

import (
	"testing"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/stretchr/testify/assert"
)

func TestGroups(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()

	groups, err := loadGroups(ctx, db, 1)
	assert.NoError(t, err)

	tcs := []struct {
		ID    GroupID
		UUID  assets.GroupUUID
		Name  string
		Query string
	}{
		{GroupID(40), assets.GroupUUID("5fc427e8-c307-49d7-91f7-8baf0db8a55e"), "Districts (Dynamic)", `district = "Faskari" OR district = "Zuru" OR district = "Anka"`},
		{GroupID(33), assets.GroupUUID("85a5a793-4741-4896-b55e-05af65f3c0fa"), "Doctors", ""},
	}

	assert.Equal(t, 10, len(groups))
	for i, tc := range tcs {
		group := groups[i].(*Group)
		assert.Equal(t, tc.UUID, group.UUID())
		assert.Equal(t, tc.ID, group.ID())
		assert.Equal(t, tc.Name, group.Name())
		assert.Equal(t, tc.Query, group.Query())
	}
}
