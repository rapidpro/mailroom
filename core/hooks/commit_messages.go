package hooks

import (
	"context"

	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

// CommitMessagesHook is our hook for comitting scene messages
var CommitMessagesHook models.EventCommitHook = &commitMessagesHook{}

type commitMessagesHook struct{}

// Apply takes care of inserting all the messages in the passed in scene assigning topups to them as needed.
func (h *commitMessagesHook) Apply(ctx context.Context, rt *runtime.Runtime, tx *sqlx.Tx, oa *models.OrgAssets, scenes map[*models.Scene][]interface{}) error {
	msgs := make([]*models.Msg, 0, len(scenes))
	for _, s := range scenes {
		for _, m := range s {
			msgs = append(msgs, m.(*models.Msg))
		}
	}

	// allocate a topup for this message if org uses topups
	topup, err := models.AllocateTopups(ctx, tx, rt.RP, oa.Org(), len(msgs))
	if err != nil {
		return errors.Wrapf(err, "error allocating topup for outgoing message")
	}

	// if we have an active topup, assign it to our messages
	if topup != models.NilTopupID {
		for _, m := range msgs {
			m.SetTopup(topup)
		}
	}

	// insert all our messages
	err = models.InsertMessages(ctx, tx, msgs)
	if err != nil {
		return errors.Wrapf(err, "error writing messages")
	}

	return nil
}
