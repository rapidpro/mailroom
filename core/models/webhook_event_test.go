package models_test

import (
	"testing"
	"time"

	"github.com/nyaruka/gocommon/dbutil/assertdb"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"

	"github.com/stretchr/testify/assert"
)

func TestWebhookEvents(t *testing.T) {
	ctx, _, db, _ := testsuite.Get()

	// create a resthook to insert against
	var resthookID models.ResthookID
	db.Get(&resthookID, `INSERT INTO api_resthook(is_active, slug, org_id, created_on, modified_on, created_by_id, modified_by_id) VALUES(TRUE, 'foo', 1, NOW(), NOW(), 1, 1) RETURNING id;`)

	tcs := []struct {
		OrgID      models.OrgID
		ResthookID models.ResthookID
		Data       string
	}{
		{testdata.Org1.ID, resthookID, `{"foo":"bar"}`},
	}

	for _, tc := range tcs {
		e := models.NewWebhookEvent(tc.OrgID, tc.ResthookID, tc.Data, time.Now())
		err := models.InsertWebhookEvents(ctx, db, []*models.WebhookEvent{e})
		assert.NoError(t, err)
		assert.NotZero(t, e.ID())

		assertdb.Query(t, db, `SELECT count(*) FROM api_webhookevent WHERE org_id = $1 AND resthook_id = $2 AND data = $3`, tc.OrgID, tc.ResthookID, tc.Data).Returns(1)
	}
}
