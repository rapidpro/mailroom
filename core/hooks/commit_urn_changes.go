package hooks

import (
	"context"

	"github.com/nyaruka/mailroom/core/models"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

// CommitURNChangesHook is our hook for when a URN is added to a contact
var CommitURNChangesHook models.EventCommitHook = &commitURNChangesHook{}

type commitURNChangesHook struct{}

// Apply adds all our URNS in a batch
func (h *commitURNChangesHook) Apply(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, oa *models.OrgAssets, scenes map[*models.Scene][]interface{}) error {
	// gather all our urn changes, we only care about the last change for each scene
	changes := make([]*models.ContactURNsChanged, 0, len(scenes))
	for _, sessionChanges := range scenes {
		changes = append(changes, sessionChanges[len(sessionChanges)-1].(*models.ContactURNsChanged))
	}

	err := models.UpdateContactURNs(ctx, tx, oa, changes)
	if err != nil {
		return errors.Wrapf(err, "error updating contact urns")
	}

	return nil
}
