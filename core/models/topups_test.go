package models_test

import (
	"testing"

	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTopups(t *testing.T) {
	ctx := testsuite.CTX()
	rt := testsuite.RT()
	db := testsuite.DB()
	rp := testsuite.RP()

	tx, err := db.BeginTxx(ctx, nil)
	assert.NoError(t, err)
	defer tx.Rollback()

	tx.MustExec(`INSERT INTO orgs_topupcredits(is_squashed, used, topup_id)
	                                    VALUES(TRUE, 1000000, 1),(TRUE, 99000, 2),(TRUE, 998, 2)`)

	tcs := []struct {
		OrgID     models.OrgID
		TopupID   models.TopupID
		Remaining int
	}{
		{testdata.Org1.ID, models.NilTopupID, 0},
		{testdata.Org2.ID, models.TopupID(2), 2},
	}

	for _, tc := range tcs {
		topup, err := models.CalculateActiveTopup(ctx, tx, tc.OrgID)
		assert.NoError(t, err)

		if tc.TopupID == models.NilTopupID {
			assert.Nil(t, topup)
		} else {
			assert.NotNil(t, topup)
			assert.Equal(t, tc.TopupID, topup.ID)
			assert.Equal(t, tc.Remaining, topup.Remaining)
		}
	}

	tc2s := []struct {
		OrgID   models.OrgID
		TopupID models.TopupID
	}{
		{testdata.Org1.ID, models.NilTopupID},
		{testdata.Org2.ID, models.TopupID(2)},
		{testdata.Org2.ID, models.TopupID(2)},
		{testdata.Org2.ID, models.NilTopupID},
	}

	for _, tc := range tc2s {
		org, err := models.LoadOrg(ctx, rt.Config, tx, tc.OrgID)
		assert.NoError(t, err)

		topup, err := models.AllocateTopups(ctx, tx, rp, org, 1)
		assert.NoError(t, err)
		assert.Equal(t, tc.TopupID, topup)
		tx.MustExec(`INSERT INTO orgs_topupcredits(is_squashed, used, topup_id) VALUES(TRUE, 1, $1)`, tc.OrgID)
	}

	// topups can be disabled for orgs
	tx.MustExec(`UPDATE orgs_org SET uses_topups = FALSE WHERE id = $1`, testdata.Org1.ID)
	org, err := models.LoadOrg(ctx, rt.Config, tx, testdata.Org1.ID)
	require.NoError(t, err)

	topup, err := models.AllocateTopups(ctx, tx, rp, org, 1)
	assert.NoError(t, err)
	assert.Equal(t, models.NilTopupID, topup)
}
