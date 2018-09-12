package models

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	null "gopkg.in/guregu/null.v3"
)

func TestTopups(t *testing.T) {
	ctx := context.Background()
	db := Reset(t)

	db.MustExec(`INSERT INTO orgs_topupcredits(is_squashed, used, topup_id)
	                                    VALUES(TRUE, 1000000, 3)`)

	tcs := []struct {
		OrgID   OrgID
		TopUpID TopUpID
	}{
		{OrgID(1), TopUpID(null.NewInt(1, true))},
		{OrgID(2), TopUpID(null.NewInt(2, true))},
		{OrgID(3), TopUpID(null.NewInt(0, false))},
	}

	for _, tc := range tcs {
		topup, err := loadActiveTopup(ctx, db, tc.OrgID)
		assert.NoError(t, err)
		assert.Equal(t, tc.TopUpID, topup)
	}
}
