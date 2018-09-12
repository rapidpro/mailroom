package campaigns

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/juju/errors"
	"github.com/nyaruka/goflow/flows"
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
	ctx, cancel := context.WithTimeout(mr.CTX, time.Minute*5)
	defer cancel()

	// decode our task body
	if task.Type != campaignEventFireType {
		return errors.Errorf("unknown event type passed to campaign worker: %s", task.Type)
	}
	eventTask := eventFireTask{}
	err := json.Unmarshal(task.Task, &eventTask)
	if err != nil {
		return errors.Annotatef(err, "error unmarshalling event fire task: %s", string(task.Task))
	}

	// first grab our event, make sure it is still unfired
	fire, err := loadEventFire(ctx, mr.DB, eventTask.FireID)
	if err != nil {
		// TODO: should we be removing this task as having been marked for execution (so it can retry?)
		return errors.Annotatef(err, "error loading event fire from db: %d", eventTask.FireID)
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

	session, err := runner.FireCampaignEvent(ctx, mr, fire.OrgID, fire.ContactID, fire.FlowUUID, &event, fire.Scheduled)
	if err != nil {
		// TODO: should we be removing this task as having been marked for execution (so it can retry?)
		return errors.Annotatef(err, "error firing campaign event: %d", eventTask.FireID)
	}

	// it this flow started ok, then mark this campaign as fired
	err = models.MarkCampaignEventFired(ctx, mr.DB, fire.FireID, session.CreatedOn)
	if err != nil {
		return errors.Annotatef(err, "error marking event fire as fired: %d", fire.FireID)
	}

	return nil
}

// EventFire represents a single campaign event fire for an event and contact
type EventFire struct {
	FireID       int               `db:"fire_id"`
	Scheduled    time.Time         `db:"scheduled"`
	Fired        *time.Time        `db:"fired"`
	ContactID    flows.ContactID   `db:"contact_id"`
	ContactUUID  flows.ContactUUID `db:"contact_uuid"`
	EventID      int64             `db:"event_id"`
	EventUUID    string            `db:"event_uuid"`
	CampaignID   int64             `db:"campaign_id"`
	CampaignUUID string            `db:"campaign_uuid"`
	CampaignName string            `db:"campaign_name"`
	OrgID        models.OrgID      `db:"org_id"`
	FlowUUID     flows.FlowUUID    `db:"flow_uuid"`
}

// loadsEventFire loads a single event fire along with the associated fields needed to run
// the fire event.
func loadEventFire(ctx context.Context, db *sqlx.DB, id int64) (*EventFire, error) {
	fire := &EventFire{}
	err := db.GetContext(ctx, fire, loadEventFireSQL, id)
	if err != nil {
		return nil, errors.Annotatef(err, "error loading event fire: %d", id)
	}
	return fire, nil
}

const loadEventFireSQL = `
SELECT 
	ef.id as fire_id, 
	ef.scheduled as scheduled, 
	ef.fired as fired, 
	c.id as contact_id, 
	c.uuid as contact_uuid,
	ce.id as event_id,
	ce.uuid as event_uuid,
	ca.id as campaign_id,
	ca.uuid as campaign_uuid,
	ca.name as campaign_name,
	ca.org_id as org_id,
	f.uuid as flow_uuid
FROM 
	campaigns_eventfire ef,
	campaigns_campaignevent ce,
	campaigns_campaign ca,
	flows_flow f,
	contacts_contact c
WHERE 
	ef.id = $1 AND
	ef.contact_id = c.id AND
	ce.id = ef.event_id AND 
	ca.id = ce.campaign_id AND
	f.id = ce.flow_id
`
