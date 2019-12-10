package models

import (
	"testing"

	"github.com/greatnonprofits-nfp/goflow/assets"
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
		{DoctorsGroupID, DoctorsGroupUUID, "Doctors", ""},
		{TestersGroupID, TestersGroupUUID, "Testers", ""},
	}

	assert.Equal(t, 2, len(groups))
	for i, tc := range tcs {
		group := groups[i].(*Group)
		assert.Equal(t, tc.UUID, group.UUID())
		assert.Equal(t, tc.ID, group.ID())
		assert.Equal(t, tc.Name, group.Name())
		assert.Equal(t, tc.Query, group.Query())
	}
}
