package campaigns

import (
	"context"
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/gocommon/analytics"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/triggers"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/runner"
	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/nyaruka/mailroom/core/tasks/handler"
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

func (t *FireCampaignEventTask) Type() string {
	return TypeFireCampaignEvent
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

	started, err := FireCampaignEvents(ctx, rt, orgID, fires, t.FlowUUID, campaign, triggers.CampaignEventUUID(t.EventUUID))

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

// FireCampaignEvents starts the flow for the passed in org, contact and flow
func FireCampaignEvents(
	ctx context.Context, rt *runtime.Runtime,
	orgID models.OrgID, fires []*models.EventFire, flowUUID assets.FlowUUID,
	campaign *triggers.CampaignReference, eventUUID triggers.CampaignEventUUID) ([]models.ContactID, error) {

	if len(fires) == 0 {
		return nil, nil
	}

	start := time.Now()

	contactIDs := make([]models.ContactID, 0, len(fires))
	fireMap := make(map[models.ContactID]*models.EventFire, len(fires))
	skippedContacts := make(map[models.ContactID]*models.EventFire, len(fires))
	for _, f := range fires {
		contactIDs = append(contactIDs, f.ContactID)
		fireMap[f.ContactID] = f
		skippedContacts[f.ContactID] = f
	}

	// create our org assets
	oa, err := models.GetOrgAssets(ctx, rt, orgID)
	if err != nil {
		return nil, errors.Wrapf(err, "error creating assets for org: %d", orgID)
	}

	// find our actual event
	dbEvent := oa.CampaignEventByID(fires[0].EventID)

	// no longer active? delete these event fires and return
	if dbEvent == nil {
		err := models.DeleteEventFires(ctx, rt.DB, fires)
		if err != nil {
			return nil, errors.Wrapf(err, "error deleting events for already fired events")
		}
		return nil, nil
	}

	// try to load our flow
	flow, err := oa.FlowByUUID(flowUUID)
	if err == models.ErrNotFound {
		err := models.DeleteEventFires(ctx, rt.DB, fires)
		if err != nil {
			return nil, errors.Wrapf(err, "error deleting events for archived or inactive flow")
		}
		return nil, nil
	}
	if err != nil {
		return nil, errors.Wrapf(err, "error loading campaign flow: %s", flowUUID)
	}
	dbFlow := flow.(*models.Flow)

	// our start options are based on the start mode for our event
	options := runner.NewStartOptions()

	switch dbEvent.StartMode() {
	case models.StartModeInterrupt:
		options.ExcludeInAFlow = false
		options.Interrupt = true
	case models.StartModePassive:
		options.ExcludeInAFlow = false
		options.Interrupt = false
	case models.StartModeSkip:
		options.ExcludeInAFlow = true
		options.Interrupt = true
	default:
		return nil, errors.Errorf("unknown start mode: %s", dbEvent.StartMode())
	}

	// if this is an ivr flow, we need to create a task to perform the start there
	if dbFlow.FlowType() == models.FlowTypeVoice {
		// Trigger our IVR flow start
		err := handler.TriggerIVRFlow(ctx, rt, oa.OrgID(), dbFlow.ID(), contactIDs, func(ctx context.Context, tx *sqlx.Tx) error {
			return models.MarkEventsFired(ctx, tx, fires, time.Now(), models.FireResultFired)
		})
		if err != nil {
			return nil, errors.Wrapf(err, "error triggering ivr flow start")
		}
		return contactIDs, nil
	}

	// our builder for the triggers that will be created for contacts
	flowRef := assets.NewFlowReference(flow.UUID(), flow.Name())
	options.TriggerBuilder = func(contact *flows.Contact) flows.Trigger {
		delete(skippedContacts, models.ContactID(contact.ID()))
		return triggers.NewBuilder(oa.Env(), flowRef, contact).Campaign(campaign, eventUUID).Build()
	}

	// this is our pre commit callback for our sessions, we'll mark the event fires associated
	// with the passed in sessions as complete in the same transaction
	fired := time.Now()
	options.CommitHook = func(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, oa *models.OrgAssets, sessions []*models.Session) error {
		// build up our list of event fire ids based on the session contact ids
		fires := make([]*models.EventFire, 0, len(sessions))
		for _, s := range sessions {
			fire, found := fireMap[s.ContactID()]
			if !found {
				return errors.Errorf("unable to find associated event fire for contact %d", s.Contact().ID())
			}
			fires = append(fires, fire)
		}

		// mark those events as fired
		err := models.MarkEventsFired(ctx, tx, fires, fired, models.FireResultFired)
		if err != nil {
			return errors.Wrapf(err, "error marking events fired")
		}

		// now build up our list of skipped contacts (no trigger was built for them)
		fires = make([]*models.EventFire, 0, len(skippedContacts))
		for _, e := range skippedContacts {
			fires = append(fires, e)
		}

		// and mark those as skipped
		err = models.MarkEventsFired(ctx, tx, fires, fired, models.FireResultSkipped)
		if err != nil {
			return errors.Wrapf(err, "error marking events skipped")
		}

		// clear those out
		skippedContacts = make(map[models.ContactID]*models.EventFire)
		return nil
	}

	sessions, err := runner.StartFlow(ctx, rt, oa, dbFlow, contactIDs, options)
	if err != nil {
		logrus.WithField("contact_ids", contactIDs).WithError(err).Errorf("error starting flow for campaign event: %s", eventUUID)
	} else {
		// make sure any skipped contacts are marked as fired this can occur if all fires were skipped
		fires := make([]*models.EventFire, 0, len(sessions))
		for _, e := range skippedContacts {
			fires = append(fires, e)
		}
		err = models.MarkEventsFired(ctx, rt.DB, fires, fired, models.FireResultSkipped)
		if err != nil {
			logrus.WithField("fire_ids", fires).WithError(err).Errorf("error marking events as skipped: %s", eventUUID)
		}
	}

	// log both our total and average
	analytics.Gauge("mr.campaign_event_elapsed", float64(time.Since(start))/float64(time.Second))
	analytics.Gauge("mr.campaign_event_count", float64(len(sessions)))

	// build the list of contacts actually started
	startedContacts := make([]models.ContactID, len(sessions))
	for i := range sessions {
		startedContacts[i] = sessions[i].ContactID()
	}
	return startedContacts, nil
}
