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
	runner.RegisterEventHandler(events.TypeContactGroupsChanged, handleContactGroupsChanged)
}

// handleContactGroupsChanged is called when a group is added or removed from our contact
func handleContactGroupsChanged(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, scene *runner.Scene, e flows.Event, userID models.UserID) error {
	event := e.(*events.ContactGroupsChanged)

	slog.Debug("contact groups changed", "contact", scene.ContactUUID(), "session", scene.SessionUUID(), "groups_removed", len(event.GroupsRemoved), "groups_added", len(event.GroupsAdded))

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
		scene.AttachPreCommitHook(hooks.UpdateContactGroups, hookEvent)
		scene.AttachPreCommitHook(hooks.UpdateCampaignFires, hookEvent)
		scene.AttachPreCommitHook(hooks.UpdateContactModifiedOn, event)
		scene.AttachPostCommitHook(hooks.IndexContacts, event)
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

		scene.AttachPreCommitHook(hooks.UpdateContactGroups, hookEvent)
		scene.AttachPreCommitHook(hooks.UpdateCampaignFires, hookEvent)
		scene.AttachPreCommitHook(hooks.UpdateContactModifiedOn, event)
		scene.AttachPostCommitHook(hooks.IndexContacts, event)
	}

	return nil
}
