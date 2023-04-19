package hooks

import (
	"context"

	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/msgio"
	"github.com/nyaruka/mailroom/runtime"
)

// SendMessagesHook is our hook for sending scene messages
var SendMessagesHook models.EventCommitHook = &sendMessagesHook{}

type sendMessagesHook struct{}

// Apply sends all non-android messages to courier
func (h *sendMessagesHook) Apply(ctx context.Context, rt *runtime.Runtime, tx *sqlx.Tx, oa *models.OrgAssets, scenes map[*models.Scene][]interface{}) error {
	msgs := make([]*models.Msg, 0, 1)

	// for each scene gather all our messages
	for _, args := range scenes {
		sceneMsgs := make([]*models.Msg, 0, 1)

		for _, m := range args {
			sceneMsgs = append(sceneMsgs, m.(*models.Msg))
		}

		// mark the last message in the sprint (used for setting timeouts)
		sceneMsgs[len(sceneMsgs)-1].LastInSprint = true

		msgs = append(msgs, sceneMsgs...)
	}

	msgio.SendMessages(ctx, rt, tx, nil, msgs)
	return nil
}
