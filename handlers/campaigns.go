package handlers

import (
	"context"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/juju/errors"
	"github.com/nyaruka/goflow/excellent/types"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/models"
	"github.com/sirupsen/logrus"
)

// UpdateCampaignEventsHook is our hook to update any campaign events
type UpdateCampaignEventsHook struct{}

var updateCampaignEventsHook = &UpdateCampaignEventsHook{}

// Apply will update all the campaigns for the passed in sessions, minimizing the number of queries to do so
func (h *UpdateCampaignEventsHook) Apply(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, orgID models.OrgID, sessions map[*models.Session][]interface{}) error {
	logrus.WithField("sessions", sessions).Debug("getting campaign callback")

	// these are all the events we need to delete unfired fires for
	deletes := make([]interface{}, 0, 5)

	// these are all the new events we need to insert
	inserts := make([]interface{}, 0, 5)

	for s, es := range sessions {
		org := s.Org()
		groupAdds := make(map[models.GroupID]bool)
		groupRemoves := make(map[models.GroupID]bool)
		fieldChanges := make(map[*models.Field]bool)

		for _, e := range es {
			switch event := e.(type) {

			case *GroupAdd:
				groupAdds[event.GroupID] = true
				delete(groupRemoves, event.GroupID)

			case *GroupRemove:
				groupRemoves[event.GroupID] = true
				delete(groupAdds, event.GroupID)

			case *events.ContactFieldChangedEvent:
				field := s.Org().FieldByKey(event.Field.Key)
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
			for _, c := range s.Org().CampaignByGroupID(g) {
				for _, e := range c.Events() {
					// TODO: filter by field value?
					deleteEvents[e.ID()] = true
				}
			}
		}

		// for every field that was changed, we need to also remove event fires and recalculate
		for f := range fieldChanges {
			fieldEvents := s.Org().CampaignEventsByFieldID(f.ID())
			for _, e := range fieldEvents {
				deleteEvents[e.ID()] = true
				addEvents[e] = true
			}
		}

		// ok, create all our deletes
		for e := range deleteEvents {
			deletes = append(deletes, &FireDelete{
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
		tz := s.Org().Env().Timezone()
		now := time.Now()
		for ce := range addEvents {
			// we aren't part of the group, move on
			if s.Contact().Groups().FindByUUID(ce.Campaign().GroupUUID()) == nil {
				continue
			}

			// get our value for the event
			value := s.Contact().Fields()[ce.RelativeToKey()]

			// no value? move on
			if value == nil {
				continue
			}

			// get the typed value
			start, isTime := value.TypedValue().(*types.XDateTime)

			// nil or not a date? move on
			if start == nil || !isTime {
				continue
			}

			// calculate our next fire
			scheduled, err := ce.ScheduleForTime(tz, now, start.Native())
			if err != nil {
				return errors.Annotatef(err, "error calculating offset for start: %s and event: %d", start, ce.ID())
			}

			// no scheduled date? move on
			if scheduled == nil {
				continue
			}

			// ok we have a new fire date, add it to our list of fires to insert
			inserts = append(inserts, &FireInsert{
				ContactID: s.Contact().ID(),
				EventID:   ce.ID(),
				Scheduled: *scheduled,
			})
		}
	}

	// first delete all our removed fires
	if len(deletes) > 0 {
		err := models.BulkInsert(ctx, tx, deleteUnfiredFires, deletes)
		if err != nil {
			return errors.Annotatef(err, "error deleting unfired event fires")
		}
	}

	// then insert our new ones
	if len(inserts) > 0 {
		err := models.BulkInsert(ctx, tx, insertFires, inserts)
		if err != nil {
			return errors.Annotatef(err, "error inserting new event fires")
		}
	}

	return nil
}

const deleteUnfiredFires = `
DELETE FROM
	campaigns_eventfire
WHERE 
	id
IN (
	SELECT 
		c.id 
	FROM 
		campaigns_eventfire c,
		(VALUES(:contact_id, :event_id)) AS f(contact_id, event_id)
	WHERE
		c.contact_id = f.contact_id::int AND c.event_id = f.event_id::int AND c.fired IS NULL
);
`

type FireDelete struct {
	ContactID flows.ContactID        `db:"contact_id"`
	EventID   models.CampaignEventID `db:"event_id"`
}

const insertFires = `
	INSERT INTO 
		campaigns_eventfire
		(contact_id, event_id, scheduled)
	VALUES(:contact_id, :event_id, :scheduled)
`

type FireInsert struct {
	ContactID flows.ContactID        `db:"contact_id"`
	EventID   models.CampaignEventID `db:"event_id"`
	Scheduled time.Time              `db:"scheduled"`
}
