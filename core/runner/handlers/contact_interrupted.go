package handlers

import (
	"context"
	"log/slog"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/runner"
	"github.com/nyaruka/mailroom/core/runner/hooks"
	"github.com/nyaruka/mailroom/runtime"
)

func init() {
	runner.RegisterEventHandler(runner.TypeContactInterrupted, handleContactInterrupted)
}

func handleContactInterrupted(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, scene *runner.Scene, e flows.Event, userID models.UserID) error {
	event := e.(*runner.ContactInterruptedEvent)

	slog.Debug("contact interrupted", "contact", scene.ContactUUID())

	scene.AttachPreCommitHook(hooks.InterruptContacts, event)
	scene.AttachPreCommitHook(hooks.UpdateContactModifiedOn, event)
	scene.AttachPostCommitHook(hooks.IndexContacts, event)

	return nil
}
