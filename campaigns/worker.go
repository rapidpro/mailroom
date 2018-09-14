package campaigns

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/juju/errors"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/triggers"
	"github.com/nyaruka/mailroom"
	"github.com/nyaruka/mailroom/marker"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/queue"
	"github.com/nyaruka/mailroom/runner"
	"github.com/sirupsen/logrus"
)

func init() {
	mailroom.AddTaskFunction(campaignEventFireType, HandleCampaignEvent)
}

// HandleCampaignEvent is called by mailroom when a campaign event task is ready to be
// processed.
func HandleCampaignEvent(mr *mailroom.Mailroom, task *queue.Task) error {
	ctx, cancel := context.WithTimeout(mr.CTX, time.Minute*5)
	defer cancel()

	return queueExpiredEventFires(ctx, mr.DB, mr.RedisPool, task)
}

// queueExpiredEventFires handles expired campaign events
// For each event:
//   - loads the event to fire
//   - loads the org asset for that event
//   - locks on the contact
//   - loads the contact for that event
//   - creates the trigger for that event
//   - runs the flow that is to be started through our engine
//   - saves the flow run and session resulting from our run
func queueExpiredEventFires(ctx context.Context, db *sqlx.DB, rp *redis.Pool, task *queue.Task) error {
	log := logrus.WithField("comp", "campaign_worker").WithField("task", string(task.Task))

	// decode our task body
	if task.Type != campaignEventFireType {
		return errors.Errorf("unknown event type passed to campaign worker: %s", task.Type)
	}
	eventTask := eventFireTask{}
	err := json.Unmarshal(task.Task, &eventTask)
	if err != nil {
		return errors.Annotatef(err, "error unmarshalling event fire task: %s", string(task.Task))
	}

	// grab all the fires for this event
	fires, err := loadEventFires(ctx, db, eventTask.FireIDs)

	// no fires returned
	if len(fires) == 0 {
		// unmark all these fires as fires so they can retry
		rc := rp.Get()
		for _, id := range eventTask.FireIDs {
			marker.RemoveTask(rc, campaignsLock, fmt.Sprintf("%d", id))
		}
		rc.Close()

		// if we had an error, return that
		if err != nil {
			return errors.Annotatef(err, "error loading event fire from db: %v", eventTask.FireIDs)
		}
		log.Info("events already fired, ignoring")
		return nil
	}

	// create our event for our campaign
	event := triggers.CampaignEvent{
		UUID: eventTask.EventUUID,
		Campaign: triggers.Campaign{
			UUID: eventTask.CampaignUUID,
			Name: eventTask.CampaignName,
		},
	}

	contactMap := make(map[flows.ContactID]*EventFire)
	contactIDs := make([]flows.ContactID, 0, len(fires))
	for _, fire := range fires {
		contactIDs = append(contactIDs, fire.ContactID)
		contactMap[fire.ContactID] = fire
	}

	sessions, err := runner.FireCampaignEvent(ctx, db, rp, eventTask.OrgID, contactIDs, eventTask.FlowUUID, &event)
	if err != nil {
		// TODO: should we be removing this task as having been marked for execution (so it can retry?)
		return errors.Annotatef(err, "error firing campaign events: %d", eventTask.FireIDs)
	}

	// TODO: optimize into a single query
	for _, session := range sessions {
		fire, found := contactMap[session.ContactID]

		// it this flow started ok, then mark this campaign as fired
		if found {
			err = models.MarkCampaignEventFired(ctx, db, fire.FireID, session.CreatedOn)
			if err != nil {
				return errors.Annotatef(err, "error marking event fire as fired: %d", fire.FireID)
			}

			// delete this contact from our map
			delete(contactMap, session.ContactID)
		} else {
			log.WithField("contact_id", session.ContactID).Errorf("unable to find session for contact id")
		}
	}

	// what remains in our contact map are fires that failed for some reason, umark these
	if len(contactMap) > 0 {
		rc := rp.Get()
		for _, failed := range contactMap {
			marker.RemoveTask(rc, campaignsLock, fmt.Sprintf("%d", failed.FireID))
		}
		rc.Close()
	}

	return nil
}

// EventFire represents a single campaign event fire for an event and contact
type EventFire struct {
	FireID    int             `db:"fire_id"`
	ContactID flows.ContactID `db:"contact_id"`
	Fired     *time.Time      `db:"fired"`
}

// loadsEventFires loads all the event fires with the passed in ids
func loadEventFires(ctx context.Context, db *sqlx.DB, ids []int64) ([]*EventFire, error) {
	q, vs, err := sqlx.In(loadEventFireSQL, ids)
	if err != nil {
		return nil, errors.Annotate(err, "error rebinding campaign fire query")
	}
	q = db.Rebind(q)

	rows, err := db.QueryxContext(ctx, q, vs...)
	if err != nil {
		return nil, errors.Annotate(err, "error querying event fires")
	}
	defer rows.Close()

	fires := make([]*EventFire, 0, len(ids))
	for rows.Next() {
		fire := &EventFire{}
		err := rows.StructScan(fire)
		if err != nil {
			return nil, errors.Annotate(err, "error scanning campaign fire")
		}
		fires = append(fires, fire)
	}
	return fires, nil
}

const loadEventFireSQL = `
SELECT 
	id as fire_id,
	contact_id as contact_id,
	fired as fired
FROM 
	campaigns_eventfire
WHERE 
	id IN(?) AND
	fired IS NULL
`
