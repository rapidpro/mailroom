package models

import (
	"testing"

	"github.com/nyaruka/mailroom/testsuite"

	"github.com/stretchr/testify/assert"
)

func TestTopups(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()
	rp := testsuite.RP()

	tx, err := db.BeginTxx(ctx, nil)
	assert.NoError(t, err)
	defer tx.Rollback()

	tx.MustExec(`INSERT INTO orgs_topupcredits(is_squashed, used, topup_id)
	                                    VALUES(TRUE, 1000000, 1),(TRUE, 99000, 2),(TRUE, 998, 2)`)

	tcs := []struct {
		OrgID     OrgID
		TopupID   TopupID
		Remaining int
	}{
		{Org1, NilTopupID, 0},
		{Org2, TopupID(2), 2},
	}

	for _, tc := range tcs {
		topup, err := calculateActiveTopup(ctx, tx, tc.OrgID)
		assert.NoError(t, err)

		if tc.TopupID == NilTopupID {
			assert.Nil(t, topup)
		} else {
			assert.NotNil(t, topup)
			assert.Equal(t, tc.TopupID, topup.ID)
			assert.Equal(t, tc.Remaining, topup.Remaining)
		}
	}

	tc2s := []struct {
		OrgID   OrgID
		TopupID TopupID
	}{
		{Org1, NilTopupID},
		{Org2, TopupID(2)},
		{Org2, TopupID(2)},
		{Org2, NilTopupID},
	}

	for _, tc := range tc2s {
		org, err := loadOrg(ctx, tx, tc.OrgID)
		assert.NoError(t, err)

		topup, err := AllocateTopups(ctx, tx, rp, org, 1)
		assert.NoError(t, err)
		assert.Equal(t, tc.TopupID, topup)
		tx.MustExec(`INSERT INTO orgs_topupcredits(is_squashed, used, topup_id) VALUES(TRUE, 1, $1)`, tc.OrgID)
	}

	// topups can be disabled for orgs
	tx.MustExec(`UPDATE orgs_org SET uses_topups = FALSE WHERE id = $1`, Org1)
	org, err := loadOrg(ctx, tx, Org1)
	topup, err := AllocateTopups(ctx, tx, rp, org, 1)
	assert.NoError(t, err)
	assert.Equal(t, NilTopupID, topup)
}
