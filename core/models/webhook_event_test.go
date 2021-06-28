package models

import (
	"testing"
	"time"

	"github.com/nyaruka/mailroom/testsuite"
	"github.com/stretchr/testify/assert"
)

func TestWebhookEvents(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()

	// create a resthook to insert against
	var resthookID ResthookID
	db.Get(&resthookID, `INSERT INTO api_resthook(is_active, slug, org_id, created_on, modified_on, created_by_id, modified_by_id) VALUES(TRUE, 'foo', 1, NOW(), NOW(), 1, 1) RETURNING id;`)

	tcs := []struct {
		OrgID      OrgID
		ResthookID ResthookID
		Data       string
	}{
		{Org1, resthookID, `{"foo":"bar"}`},
	}

	for _, tc := range tcs {
		e := NewWebhookEvent(tc.OrgID, tc.ResthookID, tc.Data, time.Now())
		err := InsertWebhookEvents(ctx, db, []*WebhookEvent{e})
		assert.NoError(t, err)
		assert.NotZero(t, e.ID())

		testsuite.AssertQueryCount(t, db, `
		SELECT count(*) FROM api_webhookevent WHERE org_id = $1 AND resthook_id = $2 AND data = $3
		`, []interface{}{tc.OrgID, tc.ResthookID, tc.Data}, 1)
	}
}
