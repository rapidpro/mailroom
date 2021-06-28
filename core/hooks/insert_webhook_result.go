package hooks

import (
	"context"

	"github.com/nyaruka/mailroom/core/models"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

// InsertWebhookResultHook is our hook for inserting webhook results
var InsertWebhookResultHook models.EventCommitHook = &insertWebhookResultHook{}

type insertWebhookResultHook struct{}

// Apply inserts all the webook results that were created
func (h *insertWebhookResultHook) Apply(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, oa *models.OrgAssets, scenes map[*models.Scene][]interface{}) error {
	// gather all our results
	results := make([]*models.WebhookResult, 0, len(scenes))
	for _, rs := range scenes {
		for _, r := range rs {
			results = append(results, r.(*models.WebhookResult))
		}
	}

	err := models.InsertWebhookResults(ctx, tx, results)
	if err != nil {
		return errors.Wrapf(err, "error inserting webhook results")
	}

	return nil
}
