package hooks

import (
	"context"

	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/msgio"
	"github.com/nyaruka/mailroom/runtime"

	"github.com/jmoiron/sqlx"
)

// SendMessagesHook is our hook for sending scene messages
var SendMessagesHook models.EventCommitHook = &sendMessagesHook{}

type sendMessagesHook struct{}

// Apply sends all non-android messages to courier
func (h *sendMessagesHook) Apply(ctx context.Context, rt *runtime.Runtime, tx *sqlx.Tx, oa *models.OrgAssets, scenes map[*models.Scene][]interface{}) error {
	msgs := make([]*models.Msg, 0, 1)

	// for each scene gather all our messages
	for s, args := range scenes {
		sceneMsgs := make([]*models.Msg, 0, 1)

		for _, m := range args {
			sceneMsgs = append(sceneMsgs, m.(*models.Msg))
		}

		// if our scene has a timeout, set it on our last message
		if len(sceneMsgs) > 0 && s.Session().Timeout() != nil && s.Session().WaitStartedOn() != nil {
			sceneMsgs[len(sceneMsgs)-1].SetTimeout(*s.Session().WaitStartedOn(), *s.Session().Timeout())
		}

		msgs = append(msgs, sceneMsgs...)
	}

	msgio.SendMessages(ctx, rt, tx, nil, msgs)
	return nil
}
