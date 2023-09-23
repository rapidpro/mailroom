package handlers

import (
	"context"
	"log/slog"

	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/core/hooks"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
)

func init() {
	models.RegisterEventHandler(events.TypeContactURNsChanged, handleContactURNsChanged)
}

// handleContactURNsChanged is called for each contact urn changed event that is encountered
func handleContactURNsChanged(ctx context.Context, rt *runtime.Runtime, tx *sqlx.Tx, oa *models.OrgAssets, scene *models.Scene, e flows.Event) error {
	event := e.(*events.ContactURNsChangedEvent)

	slog.Debug("contact urns changed", "contact", scene.ContactUUID(), "session", scene.SessionID(), "urns", event.URNs)

	// create our URN changed event
	change := &models.ContactURNsChanged{
		ContactID: scene.ContactID(),
		OrgID:     oa.OrgID(),
		URNs:      event.URNs,
	}

	scene.AppendToEventPreCommitHook(hooks.CommitURNChangesHook, change)
	scene.AppendToEventPostCommitHook(hooks.ContactModifiedHook, event)

	return nil
}
