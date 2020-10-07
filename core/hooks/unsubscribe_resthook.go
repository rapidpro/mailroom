package hooks

import (
	"context"

	"github.com/nyaruka/mailroom/core/models"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

// UnsubscribeResthookHook is our hook for when a webhook is called
var UnsubscribeResthookHook models.EventCommitHook = &unsubscribeResthookHook{}

type unsubscribeResthookHook struct{}

// Apply squashes and applies all our resthook unsubscriptions
func (h *unsubscribeResthookHook) Apply(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, oa *models.OrgAssets, scene map[*models.Scene][]interface{}) error {
	// gather all our unsubscribes
	unsubs := make([]*models.ResthookUnsubscribe, 0, len(scene))
	for _, us := range scene {
		for _, u := range us {
			unsubs = append(unsubs, u.(*models.ResthookUnsubscribe))
		}
	}

	err := models.UnsubscribeResthooks(ctx, tx, unsubs)
	if err != nil {
		return errors.Wrapf(err, "error unsubscribing from resthooks")
	}

	return nil
}
