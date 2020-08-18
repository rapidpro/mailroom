package campaigns

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nyaruka/goflow/flows/triggers"
	"github.com/nyaruka/mailroom"
	"github.com/nyaruka/mailroom/locker"
	"github.com/nyaruka/mailroom/marker"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/queue"
	"github.com/nyaruka/mailroom/runner"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func init() {
	mailroom.AddTaskFunction(queue.ScheduleCampaignEvent, HandleScheduleCampaignEvent)
	mailroom.AddTaskFunction(queue.FireCampaignEvent, HandleFireCampaignEvent)
}

// ScheduleTask is our definition of our event recalculation task
type ScheduleTask struct {
	OrgID           models.OrgID           `json:"org_id"`
	CampaignEventID models.CampaignEventID `json:"campaign_event_id"`
}

const scheduleLockKey string = "schedule_campaign_event_%d"

// HandleScheduleCampaignEvent is called by RapidPro when a campaign event has been created or updated
func HandleScheduleCampaignEvent(ctx context.Context, mr *mailroom.Mailroom, task *queue.Task) error {
	ctx, cancel := context.WithTimeout(ctx, time.Hour)
	defer cancel()

	// decode our task body
	if task.Type != queue.ScheduleCampaignEvent {
		return errors.Errorf("unknown event type passed to calc schedule campaign event worker: %s", task.Type)
	}
	t := &ScheduleTask{}
	err := json.Unmarshal(task.Task, t)
	if err != nil {
		return errors.Wrapf(err, "error unmarshalling task: %s", string(task.Task))
	}

	return ScheduleCampaignEvent(ctx, mr.DB, mr.RP, t)
}

// ScheduleCampaignEvent creates the actual event fires to schedule the given campaign event
func ScheduleCampaignEvent(ctx context.Context, db *sqlx.DB, rp *redis.Pool, t *ScheduleTask) error {
	lockKey := fmt.Sprintf(scheduleLockKey, t.CampaignEventID)
	lock, err := locker.GrabLock(rp, lockKey, time.Hour, time.Minute*5)
	if err != nil {
		return errors.Wrapf(err, "error grabbing lock to schedule campaign event %d", t.CampaignEventID)
	}
	defer locker.ReleaseLock(rp, lockKey, lock)

	err = models.ScheduleCampaignEvent(ctx, db, t.OrgID, t.CampaignEventID)
	if err != nil {
		return errors.Wrapf(err, "error scheduling campaign event %d", t.CampaignEventID)
	}

	return nil
}

// HandleFireCampaignEvent is called by mailroom when a campaign event task is ready to be processed.
func HandleFireCampaignEvent(ctx context.Context, mr *mailroom.Mailroom, task *queue.Task) error {
	ctx, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	return fireEventFires(ctx, mr.DB, mr.RP, task)
}

// fireEventFires handles expired campaign events
// For each event:
//   - loads the event to fire
//   - loads the org asset for that event
//   - locks on the contact
//   - loads the contact for that event
//   - creates the trigger for that event
//   - runs the flow that is to be started through our engine
//   - saves the flow run and session resulting from our run
func fireEventFires(ctx context.Context, db *sqlx.DB, rp *redis.Pool, task *queue.Task) error {
	log := logrus.WithField("comp", "campaign_worker").WithField("task", string(task.Task))

	// decode our task body
	if task.Type != queue.FireCampaignEvent {
		return errors.Errorf("unknown event type passed to campaign worker: %s", task.Type)
	}
	eventTask := eventFireTask{}
	err := json.Unmarshal(task.Task, &eventTask)
	if err != nil {
		return errors.Wrapf(err, "error unmarshalling event fire task: %s", string(task.Task))
	}

	// grab all the fires for this event
	fires, err := models.LoadEventFires(ctx, db, eventTask.FireIDs)
	if err != nil {
		// unmark all these fires as fires so they can retry
		rc := rp.Get()
		for _, id := range eventTask.FireIDs {
			rerr := marker.RemoveTask(rc, campaignsLock, fmt.Sprintf("%d", id))
			if rerr != nil {
				log.WithError(rerr).WithField("fire_id", id).Error("error unmarking campaign fire")
			}
		}
		rc.Close()

		// if we had an error, return that
		return errors.Wrapf(err, "error loading event fire from db: %v", eventTask.FireIDs)
	}

	// no fires returned
	if len(fires) == 0 {
		log.Info("events already fired, ignoring")
		return nil
	}

	contactMap := make(map[models.ContactID]*models.EventFire)
	for _, fire := range fires {
		contactMap[fire.ContactID] = fire
	}

	campaign := triggers.NewCampaignReference(triggers.CampaignUUID(eventTask.CampaignUUID), eventTask.CampaignName)

	started, err := runner.FireCampaignEvents(ctx, db, rp, eventTask.OrgID, fires, eventTask.FlowUUID, campaign, triggers.CampaignEventUUID(eventTask.EventUUID))

	// remove all the contacts that were started
	for _, contactID := range started {
		delete(contactMap, contactID)
	}

	// what remains in our contact map are fires that failed for some reason, umark these
	if len(contactMap) > 0 {
		rc := rp.Get()
		for _, failed := range contactMap {
			marker.RemoveTask(rc, campaignsLock, fmt.Sprintf("%d", failed.FireID))
		}
		rc.Close()
	}

	if err != nil {
		return errors.Wrapf(err, "error firing campaign events: %d", eventTask.FireIDs)
	}

	return nil
}
