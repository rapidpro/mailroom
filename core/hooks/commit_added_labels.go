package hooks

import (
	"context"
	"fmt"

	"github.com/nyaruka/mailroom/core/models"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
)

// CommitAddedLabelsHook is our hook for input labels being added
var CommitAddedLabelsHook models.EventCommitHook = &commitAddedLabelsHook{}

type commitAddedLabelsHook struct{}

// Apply applies our input labels added, committing them in a single batch
func (h *commitAddedLabelsHook) Apply(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, oa *models.OrgAssets, scenes map[*models.Scene][]interface{}) error {
	// build our list of msg label adds, we dedupe these so we never double add in the same transaction
	seen := make(map[string]bool)
	adds := make([]*models.MsgLabelAdd, 0, len(scenes))

	for _, as := range scenes {
		for _, a := range as {
			add := a.(*models.MsgLabelAdd)
			key := fmt.Sprintf("%d:%d", add.LabelID, add.MsgID)
			if !seen[key] {
				adds = append(adds, add)
				seen[key] = true
			}
		}
	}

	// insert our adds
	return models.AddMsgLabels(ctx, tx, adds)
}
