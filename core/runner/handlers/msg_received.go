package handlers

import (
	"context"
	"log/slog"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/runner"
	"github.com/nyaruka/mailroom/core/runner/hooks"
	"github.com/nyaruka/mailroom/core/search"
	"github.com/nyaruka/mailroom/runtime"
)

func init() {
	runner.RegisterEventHandler(events.TypeMsgReceived, handleMsgReceived)
}

// handleMsgReceived takes care of update last seen on and any campaigns based on that
func handleMsgReceived(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, scene *runner.Scene, e flows.Event, userID models.UserID) error {
	event := e.(*events.MsgReceived)

	slog.Debug("msg received", "contact", scene.ContactUUID(), "session", scene.SessionUUID(), "text", event.Msg.Text(), "urn", event.Msg.URN())

	// update the message to be handled
	if scene.IncomingMsg != nil && !scene.IncomingMsg.Handled {
		scene.AttachPreCommitHook(hooks.UpdateMessageHandled, event)
	}

	// index message to OpenSearch if it has text and an associated ticket
	if event.TicketUUID != "" && len(event.Msg.Text()) >= search.MessageTextMinLength {
		scene.AttachPostCommitHook(hooks.IndexMessages, &search.MessageDoc{
			CreatedOn:   event.CreatedOn(),
			OrgID:       oa.OrgID(),
			UUID:        event.UUID(),
			ContactUUID: scene.ContactUUID(),
			Text:        event.Msg.Text(),
		})
	}

	return nil
}
