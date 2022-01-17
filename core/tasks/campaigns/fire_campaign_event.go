package campaigns

import (
	"context"
	"fmt"
	"time"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows/triggers"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/runner"
	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/nyaruka/mailroom/runtime"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// TypeFireCampaignEvent is the type of the fire event task
const TypeFireCampaignEvent = "fire_campaign_event"

func init() {
	tasks.RegisterType(TypeFireCampaignEvent, func() tasks.Task { return &FireCampaignEventTask{} })
}

// FireCampaignEventTask is the task to handle firing campaign events
type FireCampaignEventTask struct {
	FireIDs      []models.FireID `json:"fire_ids"`
	EventID      int64           `json:"event_id"`
	EventUUID    string          `json:"event_uuid"`
	FlowUUID     assets.FlowUUID `json:"flow_uuid"`
	CampaignUUID string          `json:"campaign_uuid"`
	CampaignName string          `json:"campaign_name"`
}

// Timeout is the maximum amount of time the task can run for
func (t *FireCampaignEventTask) Timeout() time.Duration {
	// base of 5 minutes + one minute per fire
	return time.Minute*5 + time.Minute*time.Duration(len(t.FireIDs))
}

// Perform handles firing campaign events
//   - loads the org assets for that event
//   - locks on the contact
//   - loads the contact for that event
//   - creates the trigger for that event
//   - runs the flow that is to be started through our engine
//   - saves the flow run and session resulting from our run
func (t *FireCampaignEventTask) Perform(ctx context.Context, rt *runtime.Runtime, orgID models.OrgID) error {
	db := rt.DB
	rp := rt.RP
	log := logrus.WithField("comp", "campaign_worker").WithField("event_id", t.EventID)

	// grab all the fires for this event
	fires, err := models.LoadEventFires(ctx, db, t.FireIDs)
	if err != nil {
		// unmark all these fires as fires so they can retry
		rc := rp.Get()
		for _, id := range t.FireIDs {
			rerr := campaignsMarker.Remove(rc, fmt.Sprintf("%d", id))
			if rerr != nil {
				log.WithError(rerr).WithField("fire_id", id).Error("error unmarking campaign fire")
			}
		}
		rc.Close()

		// if we had an error, return that
		return errors.Wrapf(err, "error loading event fire from db: %v", t.FireIDs)
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

	campaign := triggers.NewCampaignReference(triggers.CampaignUUID(t.CampaignUUID), t.CampaignName)

	started, err := runner.FireCampaignEvents(ctx, rt, orgID, fires, t.FlowUUID, campaign, triggers.CampaignEventUUID(t.EventUUID))

	// remove all the contacts that were started
	for _, contactID := range started {
		delete(contactMap, contactID)
	}

	// what remains in our contact map are fires that failed for some reason, umark these
	if len(contactMap) > 0 {
		rc := rp.Get()
		for _, failed := range contactMap {
			rerr := campaignsMarker.Remove(rc, fmt.Sprintf("%d", failed.FireID))
			if rerr != nil {
				log.WithError(rerr).WithField("fire_id", failed.FireID).Error("error unmarking campaign fire")
			}
		}
		rc.Close()
	}

	if err != nil {
		return errors.Wrapf(err, "error firing campaign events: %d", t.FireIDs)
	}

	return nil
}
