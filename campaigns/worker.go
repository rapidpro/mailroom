package campaigns

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nyaruka/goflow/flows/triggers"
	"github.com/nyaruka/mailroom"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/queue"
	"github.com/nyaruka/mailroom/runner"
	"github.com/sirupsen/logrus"
)

func init() {
	mailroom.AddTaskFunction(campaignEventFireType, HandleCampaignEvent)
}

// HandleCampaignEvent handles campaign event fires.
// For each event:
//   - loads the event to fire
//   - loads the org asset for that event
//   - locks on the contact
//   - loads the contact for that event
//   - creates the trigger for that event
//   - runs the flow that is to be started through our engine
//   - saves the flow run and session resulting from our run
func HandleCampaignEvent(mr *mailroom.Mailroom, task *queue.Task) error {
	log := logrus.WithField("comp", "campaign_worker").WithField("task", task.Task)
	ctx, cancel := context.WithTimeout(mr.CTX, time.Minute)
	defer cancel()

	// decode our task body
	if task.Type != campaignEventFireType {
		err := fmt.Errorf("unknown event type passed to campaign worker: %s", task.Type)
		log.WithError(err).Error("error handling event")
		return err
	}

	eventTask := eventFireTask{}
	err := json.Unmarshal(task.Task, &eventTask)
	if err != nil {
		log.WithError(err).Error("error unmarshalling task")
		return err
	}

	// first grab our event, make sure it is still unfired
	fire, err := loadEventFire(ctx, mr.DB, eventTask.FireID)
	if err != nil {
		log.WithError(err).Error("error loading event fire from db")
		return err
	}

	// if it is already fired, that's ok, just exit
	if fire.Fired != nil {
		log.Info("event already fired, ignoring")
		return nil
	}

	// create our event for our campaign
	event := triggers.CampaignEvent{
		UUID: fire.EventUUID,
		Campaign: triggers.Campaign{
			UUID: fire.CampaignUUID,
			Name: fire.CampaignName,
		},
	}

	session, err := runner.FireCampaignEvent(mr, fire.OrgID, fire.ContactID, fire.FlowUUID, &event, fire.Scheduled)
	if err != nil {
		log.WithError(err).Error("error firing campaign")
		return err
	}

	// it this flow started ok, then mark this campaign as fired
	err = models.MarkCampaignEventFired(ctx, mr.DB, fire.FireID, session.CreatedOn)
	if err != nil {
		log.WithError(err).Error("error marking event fire as fired")
		return err
	}

	return nil
}
