package hooks

import (
	"context"

	"github.com/nyaruka/mailroom/core/models"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

// InsertWebhookEventHook is our hook for when a resthook needs to be inserted
var InsertWebhookEventHook models.EventCommitHook = &insertWebhookEventHook{}

type insertWebhookEventHook struct{}

// Apply inserts all the webook events that were created
func (h *insertWebhookEventHook) Apply(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, oa *models.OrgAssets, scenes map[*models.Scene][]interface{}) error {
	events := make([]*models.WebhookEvent, 0, len(scenes))
	for _, rs := range scenes {
		for _, r := range rs {
			events = append(events, r.(*models.WebhookEvent))
		}
	}

	err := models.InsertWebhookEvents(ctx, tx, events)
	if err != nil {
		return errors.Wrapf(err, "error inserting webhook events")
	}

	return nil
}
