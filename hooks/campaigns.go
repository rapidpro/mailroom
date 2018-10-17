package hooks

import (
	"context"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/models"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// UpdateCampaignEventsHook is our hook to update any campaign events
type UpdateCampaignEventsHook struct{}

var updateCampaignEventsHook = &UpdateCampaignEventsHook{}

// Apply will update all the campaigns for the passed in sessions, minimizing the number of queries to do so
func (h *UpdateCampaignEventsHook) Apply(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, sessions map[*models.Session][]interface{}) error {
	// these are all the events we need to delete unfired fires for
	deletes := make([]*models.FireDelete, 0, 5)

	// these are all the new events we need to insert
	inserts := make([]*models.FireAdd, 0, 5)

	for s, es := range sessions {
		groupAdds := make(map[models.GroupID]bool)
		groupRemoves := make(map[models.GroupID]bool)
		fieldChanges := make(map[*models.Field]bool)

		for _, e := range es {
			switch event := e.(type) {

			case *models.GroupAdd:
				groupAdds[event.GroupID] = true
				delete(groupRemoves, event.GroupID)

			case *models.GroupRemove:
				groupRemoves[event.GroupID] = true
				delete(groupAdds, event.GroupID)

			case *events.ContactFieldChangedEvent:
				field := org.FieldByKey(event.Field.Key)
				if field == nil {
					logrus.WithFields(logrus.Fields{
						"field_key":  event.Field.Key,
						"field_name": event.Field.Name,
						"session_id": s.ID,
					}).Debug("unable to find field with key, ignoring for campaign updates")
					continue
				}
				fieldChanges[field] = true
			}
		}

		// those events that need deleting
		deleteEvents := make(map[models.CampaignEventID]bool, len(groupRemoves)+len(fieldChanges))

		// those events we need to add
		addEvents := make(map[*models.CampaignEvent]bool, len(groupAdds)+len(fieldChanges))

		// for every group that was removed, we need to remove all event fires for them
		for g := range groupRemoves {
			for _, c := range org.CampaignByGroupID(g) {
				for _, e := range c.Events() {
					// TODO: filter by field value?
					deleteEvents[e.ID()] = true
				}
			}
		}

		// for every field that was changed, we need to also remove event fires and recalculate
		for f := range fieldChanges {
			fieldEvents := org.CampaignEventsByFieldID(f.ID())
			for _, e := range fieldEvents {
				deleteEvents[e.ID()] = true
				addEvents[e] = true
			}
		}

		// ok, create all our deletes
		for e := range deleteEvents {
			deletes = append(deletes, &models.FireDelete{
				ContactID: s.ContactID,
				EventID:   e,
			})
		}

		// add in all the events we qualify for in campaigns we are now part of
		for g := range groupAdds {
			for _, c := range org.CampaignByGroupID(g) {
				for _, e := range c.Events() {
					addEvents[e] = true
				}
			}
		}

		// ok, for all the unique events we now calculate our fire date
		tz := org.Env().Timezone()
		now := time.Now()
		for ce := range addEvents {
			scheduled, err := ce.ScheduleForContact(tz, now, s.Contact())
			if err != nil {
				return errors.Wrapf(err, "error calculating offset")
			}

			// no scheduled date? move on
			if scheduled == nil {
				continue
			}

			// ok we have a new fire date, add it to our list of fires to insert
			inserts = append(inserts, &models.FireAdd{
				ContactID: s.Contact().ID(),
				EventID:   ce.ID(),
				Scheduled: *scheduled,
			})
		}
	}

	// first delete all our removed fires
	err := models.DeleteUnfiredEventFires(ctx, tx, deletes)
	if err != nil {
		return errors.Wrapf(err, "error deleting unfired event fires")
	}

	// then insert our new ones
	err = models.AddEventFires(ctx, tx, inserts)
	if err != nil {
		return errors.Wrapf(err, "error inserting new event fires")
	}

	return nil
}
