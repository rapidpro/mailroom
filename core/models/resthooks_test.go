package models_test

import (
	"testing"

	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResthooks(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	rt.DB.MustExec(`INSERT INTO api_resthook(is_active, created_on, modified_on, slug, created_by_id, modified_by_id, org_id)
								   VALUES(TRUE, NOW(), NOW(), 'registration', 1, 1, 1);`)
	rt.DB.MustExec(`INSERT INTO api_resthook(is_active, created_on, modified_on, slug, created_by_id, modified_by_id, org_id)
								   VALUES(TRUE, NOW(), NOW(), 'block', 1, 1, 1);`)
	rt.DB.MustExec(`INSERT INTO api_resthooksubscriber(is_active, created_on, modified_on, target_url, created_by_id, modified_by_id, resthook_id)
											 VALUES(TRUE, NOW(), NOW(), 'https://foo.bar', 1, 1, 2);`)
	rt.DB.MustExec(`INSERT INTO api_resthooksubscriber(is_active, created_on, modified_on, target_url, created_by_id, modified_by_id, resthook_id)
	                                         VALUES(TRUE, NOW(), NOW(), 'https://bar.foo', 1, 1, 2);`)

	oa, err := models.GetOrgAssetsWithRefresh(ctx, rt, testdata.Org1.ID, models.RefreshResthooks)
	require.NoError(t, err)

	resthooks, err := oa.Resthooks()
	require.NoError(t, err)

	tcs := []struct {
		ID          models.ResthookID
		Slug        string
		Subscribers []string
	}{
		{models.ResthookID(2), "block", []string{"https://bar.foo", "https://foo.bar"}},
		{models.ResthookID(1), "registration", nil},
	}

	assert.Equal(t, 2, len(resthooks))
	for i, tc := range tcs {
		resthook := resthooks[i].(*models.Resthook)
		assert.Equal(t, tc.ID, resthook.ID())
		assert.Equal(t, tc.Slug, resthook.Slug())
		assert.Equal(t, tc.Subscribers, resthook.Subscribers())
	}
}
