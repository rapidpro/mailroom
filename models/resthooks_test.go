package models

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestResthooks(t *testing.T) {
	ctx := context.Background()
	db := Reset(t)

	db.MustExec(`INSERT INTO api_resthook(is_active, created_on, modified_on, slug, created_by_id, modified_by_id, org_id)
								   VALUES(TRUE, NOW(), NOW(), 'registration', 1, 1, 1);`)
	db.MustExec(`INSERT INTO api_resthook(is_active, created_on, modified_on, slug, created_by_id, modified_by_id, org_id)
								   VALUES(TRUE, NOW(), NOW(), 'block', 1, 1, 1);`)
	db.MustExec(`INSERT INTO api_resthooksubscriber(is_active, created_on, modified_on, target_url, created_by_id, modified_by_id, resthook_id)
											 VALUES(TRUE, NOW(), NOW(), 'https://foo.bar', 1, 1, 2);`)
	db.MustExec(`INSERT INTO api_resthooksubscriber(is_active, created_on, modified_on, target_url, created_by_id, modified_by_id, resthook_id)
	                                         VALUES(TRUE, NOW(), NOW(), 'https://bar.foo', 1, 1, 2);`)

	resthooks, err := loadResthooks(ctx, db, 1)
	assert.NoError(t, err)

	tcs := []struct {
		ID          ResthookID
		Slug        string
		Subscribers []string
	}{
		{ResthookID(2), "block", []string{"https://bar.foo", "https://foo.bar"}},
		{ResthookID(1), "registration", nil},
	}

	assert.Equal(t, 2, len(resthooks))
	for i, tc := range tcs {
		assert.Equal(t, tc.ID, resthooks[i].ID())
		assert.Equal(t, tc.Slug, resthooks[i].Slug())
		assert.Equal(t, tc.Subscribers, resthooks[i].Subscribers())
	}
}
