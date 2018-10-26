package hooks

import (
	"context"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/models"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func init() {
	models.RegisterEventHook(events.TypeContactGroupsChanged, handleContactGroupsChanged)
}

// CommitGroupChangesHook is our hook for all group changes
type CommitGroupChangesHook struct{}

var commitGroupChangesHook = &CommitGroupChangesHook{}

// Apply squashes and adds or removes all our contact groups
func (h *CommitGroupChangesHook) Apply(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, sessions map[*models.Session][]interface{}) error {
	// build up our list of all adds and removes
	adds := make([]*models.GroupAdd, 0, len(sessions))
	removes := make([]*models.GroupRemove, 0, len(sessions))
	changed := make(map[flows.ContactID]bool, len(sessions))

	// we remove from our groups at once, build up our list
	for _, events := range sessions {
		// we use these sets to track what our final add or remove should be
		seenAdds := make(map[models.GroupID]*models.GroupAdd)
		seenRemoves := make(map[models.GroupID]*models.GroupRemove)

		for _, e := range events {
			switch event := e.(type) {
			case *models.GroupAdd:
				seenAdds[event.GroupID] = event
				delete(seenRemoves, event.GroupID)
			case *models.GroupRemove:
				seenRemoves[event.GroupID] = event
				delete(seenAdds, event.GroupID)
			}
		}

		for _, add := range seenAdds {
			adds = append(adds, add)
			changed[add.ContactID] = true
		}

		for _, remove := range seenRemoves {
			removes = append(removes, remove)
			changed[remove.ContactID] = true
		}
	}

	// do our updates
	err := models.AddContactsToGroups(ctx, tx, adds)
	if err != nil {
		return errors.Wrapf(err, "error adding contacts to groups")
	}

	err = models.RemoveContactsFromGroups(ctx, tx, removes)
	if err != nil {
		return errors.Wrapf(err, "error removing contacts from groups")
	}

	// build the list of all contact ids changed, we'll update modified_on for them
	changedIDs := make([]flows.ContactID, 0, len(changed))
	for c := range changed {
		changedIDs = append(changedIDs, c)
	}
	if len(changedIDs) > 0 {
		_, err = tx.ExecContext(ctx, `UPDATE contacts_contact SET modified_on = NOW() WHERE id = ANY($1)`, pq.Array(changedIDs))
		if err != nil {
			return errors.Wrapf(err, "error updating contacts modified_on")
		}
	}

	return nil
}

// handleContactGroupsChanged is called when a group is added or removed from our contact
func handleContactGroupsChanged(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, session *models.Session, e flows.Event) error {
	event := e.(*events.ContactGroupsChangedEvent)
	logrus.WithFields(logrus.Fields{
		"contact_uuid":   session.ContactUUID(),
		"session_id":     session.ID,
		"groups_removed": len(event.GroupsRemoved),
		"groups_added":   len(event.GroupsAdded),
	}).Debug("changing contact groups")

	// remove each of our groups
	for _, g := range event.GroupsRemoved {
		// look up our group id
		group := org.GroupByUUID(g.UUID)
		if group == nil {
			logrus.WithFields(logrus.Fields{
				"contact_uuid": session.ContactUUID(),
				"group_uuid":   g.UUID,
			}).Warn("unable to find group to remove, skipping")
			continue
		}

		hookEvent := &models.GroupRemove{
			ContactID: session.Contact().ID(),
			GroupID:   group.ID(),
		}

		// add our add event
		session.AddPreCommitEvent(commitGroupChangesHook, hookEvent)
		session.AddPreCommitEvent(updateCampaignEventsHook, hookEvent)
		session.AddPreCommitEvent(contactModifiedHook, session.Contact().ID())
	}

	// add each of our groups
	for _, g := range event.GroupsAdded {
		// look up our group id
		group := org.GroupByUUID(g.UUID)
		if group == nil {
			logrus.WithFields(logrus.Fields{
				"contact_uuid": session.ContactUUID(),
				"group_uuid":   g.UUID,
			}).Warn("unable to find group to add, skipping")
			continue
		}

		// add our add event
		hookEvent := &models.GroupAdd{
			ContactID: session.Contact().ID(),
			GroupID:   group.ID(),
		}

		session.AddPreCommitEvent(commitGroupChangesHook, hookEvent)
		session.AddPreCommitEvent(updateCampaignEventsHook, hookEvent)
		session.AddPreCommitEvent(contactModifiedHook, session.Contact().ID())
	}

	return nil
}
