package models

import (
	"testing"

	"github.com/nyaruka/mailroom/testsuite"
	"github.com/stretchr/testify/assert"
)

func TestResthooks(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()

	tx, err := db.BeginTxx(ctx, nil)
	assert.NoError(t, err)
	defer tx.Rollback()

	tx.MustExec(`INSERT INTO api_resthook(is_active, created_on, modified_on, slug, created_by_id, modified_by_id, org_id)
								   VALUES(TRUE, NOW(), NOW(), 'registration', 1, 1, 1);`)
	tx.MustExec(`INSERT INTO api_resthook(is_active, created_on, modified_on, slug, created_by_id, modified_by_id, org_id)
								   VALUES(TRUE, NOW(), NOW(), 'block', 1, 1, 1);`)
	tx.MustExec(`INSERT INTO api_resthooksubscriber(is_active, created_on, modified_on, target_url, created_by_id, modified_by_id, resthook_id)
											 VALUES(TRUE, NOW(), NOW(), 'https://foo.bar', 1, 1, 2);`)
	tx.MustExec(`INSERT INTO api_resthooksubscriber(is_active, created_on, modified_on, target_url, created_by_id, modified_by_id, resthook_id)
	                                         VALUES(TRUE, NOW(), NOW(), 'https://bar.foo', 1, 1, 2);`)

	resthooks, err := loadResthooks(ctx, tx, 1)
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
		resthook := resthooks[i].(*Resthook)
		assert.Equal(t, tc.ID, resthook.ID())
		assert.Equal(t, tc.Slug, resthook.Slug())
		assert.Equal(t, tc.Subscribers, resthook.Subscribers())
	}
}
