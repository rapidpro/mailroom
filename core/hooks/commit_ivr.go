package hooks

import (
	"context"

	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

// CommitIVRHook is our hook for comitting scene messages / say commands
var CommitIVRHook models.EventCommitHook = &commitIVRHook{}

type commitIVRHook struct{}

// Apply takes care of inserting all the messages in the passed in scene.
func (h *commitIVRHook) Apply(ctx context.Context, rt *runtime.Runtime, tx *sqlx.Tx, oa *models.OrgAssets, scenes map[*models.Scene][]interface{}) error {
	msgs := make([]*models.Msg, 0, len(scenes))
	for _, s := range scenes {
		for _, m := range s {
			msgs = append(msgs, m.(*models.Msg))
		}
	}

	// insert all our messages
	if err := models.InsertMessages(ctx, tx, msgs); err != nil {
		return errors.Wrapf(err, "error writing messages")
	}

	return nil
}
