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
	runner.RegisterEventHandler(events.TypeRunStarted, handleRunStarted)
}

func handleRunStarted(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, scene *runner.Scene, e flows.Event, userID models.UserID) error {
	event := e.(*events.RunStarted)

	slog.Debug("run started", "contact", scene.ContactUUID(), "session", scene.SessionUUID(), slog.Group("flow", "uuid", event.Flow.UUID, "name", event.Flow.Name))

	// we've potentially changed contact flow history.. only way to be sure would be loading contacts with their
	// flow history, but not sure that is worth it given how likely we are to be updating modified_on anyway
	scene.AttachPreCommitHook(hooks.UpdateContactModifiedOn, event)
	scene.AttachPostCommitHook(hooks.IndexContacts, event)

	return nil
}
