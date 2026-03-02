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
	runner.RegisterEventHandler(events.TypeContactURNsChanged, handleContactURNsChanged)
}

// handleContactURNsChanged is called for each contact urn changed event that is encountered
func handleContactURNsChanged(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, scene *runner.Scene, e flows.Event, userID models.UserID) error {
	event := e.(*events.ContactURNsChanged)

	slog.Debug("contact urns changed", "contact", scene.ContactUUID(), "session", scene.SessionUUID(), "urns", event.URNs)

	// create our URN changed event
	change := &models.ContactURNsChanged{Contact: scene.DBContact, URNs: event.URNs}

	scene.AttachPreCommitHook(hooks.UpdateContactURNs, change)
	scene.AttachPreCommitHook(hooks.UpdateContactModifiedOn, event)
	scene.AttachPostCommitHook(hooks.IndexContacts, event)

	return nil
}
