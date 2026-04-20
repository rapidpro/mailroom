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
	models.RegisterEventHandler(events.TypeContactGroupsChanged, handleContactGroupsChanged)
}

// handleContactGroupsChanged is called when a group is added or removed from our contact
func handleContactGroupsChanged(ctx context.Context, rt *runtime.Runtime, tx *sqlx.Tx, oa *models.OrgAssets, scene *models.Scene, e flows.Event) error {
	event := e.(*events.ContactGroupsChangedEvent)

	slog.Debug("contact groups changed", "contact", scene.ContactUUID(), "session", scene.SessionID(), "groups_removed", len(event.GroupsRemoved), "groups_added", len(event.GroupsAdded))

	// remove each of our groups
	for _, g := range event.GroupsRemoved {
		// look up our group id
		group := oa.GroupByUUID(g.UUID)
		if group == nil {
			slog.Warn("unable to find group to remove, skipping", "contact", scene.ContactUUID(), "group", g.UUID)
			continue
		}

		hookEvent := &models.GroupRemove{
			ContactID: scene.ContactID(),
			GroupID:   group.ID(),
		}

		// add our add event
		scene.AppendToEventPreCommitHook(hooks.CommitGroupChangesHook, hookEvent)
		scene.AppendToEventPreCommitHook(hooks.UpdateCampaignEventsHook, hookEvent)
		scene.AppendToEventPostCommitHook(hooks.ContactModifiedHook, event)
	}

	// add each of our groups
	for _, g := range event.GroupsAdded {
		// look up our group id
		group := oa.GroupByUUID(g.UUID)
		if group == nil {
			slog.Warn("unable to find group to add, skipping", "contact", scene.ContactUUID(), "group", g.UUID)
			continue
		}

		// add our add event
		hookEvent := &models.GroupAdd{
			ContactID: scene.ContactID(),
			GroupID:   group.ID(),
		}

		scene.AppendToEventPreCommitHook(hooks.CommitGroupChangesHook, hookEvent)
		scene.AppendToEventPreCommitHook(hooks.UpdateCampaignEventsHook, hookEvent)
		scene.AppendToEventPostCommitHook(hooks.ContactModifiedHook, event)
	}

	return nil
}
