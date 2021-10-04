package handlers

import (
	"context"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/core/hooks"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"

	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"
)

func init() {
	models.RegisterEventHandler(events.TypeContactGroupsChanged, handleContactGroupsChanged)
}

// handleContactGroupsChanged is called when a group is added or removed from our contact
func handleContactGroupsChanged(ctx context.Context, rt *runtime.Runtime, tx *sqlx.Tx, oa *models.OrgAssets, scene *models.Scene, e flows.Event) error {
	event := e.(*events.ContactGroupsChangedEvent)
	logrus.WithFields(logrus.Fields{
		"contact_uuid":   scene.ContactUUID(),
		"session_id":     scene.SessionID(),
		"groups_removed": len(event.GroupsRemoved),
		"groups_added":   len(event.GroupsAdded),
	}).Debug("changing contact groups")

	// remove each of our groups
	for _, g := range event.GroupsRemoved {
		// look up our group id
		group := oa.GroupByUUID(g.UUID)
		if group == nil {
			logrus.WithFields(logrus.Fields{
				"contact_uuid": scene.ContactUUID(),
				"group_uuid":   g.UUID,
			}).Warn("unable to find group to remove, skipping")
			continue
		}

		hookEvent := &models.GroupRemove{
			ContactID: scene.ContactID(),
			GroupID:   group.ID(),
		}

		// add our add event
		scene.AppendToEventPreCommitHook(hooks.CommitGroupChangesHook, hookEvent)
		scene.AppendToEventPreCommitHook(hooks.UpdateCampaignEventsHook, hookEvent)
		scene.AppendToEventPreCommitHook(hooks.ContactModifiedHook, scene.ContactID())
	}

	// add each of our groups
	for _, g := range event.GroupsAdded {
		// look up our group id
		group := oa.GroupByUUID(g.UUID)
		if group == nil {
			logrus.WithFields(logrus.Fields{
				"contact_uuid": scene.ContactUUID(),
				"group_uuid":   g.UUID,
			}).Warn("unable to find group to add, skipping")
			continue
		}

		// add our add event
		hookEvent := &models.GroupAdd{
			ContactID: scene.ContactID(),
			GroupID:   group.ID(),
		}

		scene.AppendToEventPreCommitHook(hooks.CommitGroupChangesHook, hookEvent)
		scene.AppendToEventPreCommitHook(hooks.UpdateCampaignEventsHook, hookEvent)
		scene.AppendToEventPreCommitHook(hooks.ContactModifiedHook, scene.ContactID())
	}

	return nil
}
