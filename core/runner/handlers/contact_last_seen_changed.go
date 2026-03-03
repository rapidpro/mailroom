package handlers

import (
	"context"
	"log/slog"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/runner"
	"github.com/nyaruka/mailroom/core/runner/hooks"
	"github.com/nyaruka/mailroom/runtime"
)

func init() {
	runner.RegisterEventHandler(events.TypeContactLastSeenChanged, handleContactLastSeenChanged)
}

func handleContactLastSeenChanged(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, scene *runner.Scene, e flows.Event, userID models.UserID) error {
	event := e.(*events.ContactLastSeenChanged)

	slog.Debug("contact last seen changed", "contact", scene.ContactUUID(), "session", scene.SessionUUID(), "seen", event.LastSeenOn)

	scene.AttachPreCommitHook(hooks.UpdateContactLastSeenOn, event)
	scene.AttachPreCommitHook(hooks.UpdateCampaignFires, event)
	scene.AttachPreCommitHook(hooks.UpdateContactModifiedOn, event)
	scene.AttachPostCommitHook(hooks.IndexContacts, event)

	return nil
}
