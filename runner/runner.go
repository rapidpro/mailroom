package runner

import (
	"context"
	"time"

	"github.com/juju/errors"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows/triggers"
	"github.com/nyaruka/mailroom"
	"github.com/nyaruka/mailroom/courier"
	"github.com/nyaruka/mailroom/models"
	"github.com/sirupsen/logrus"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/utils"

	"github.com/nyaruka/goflow/flows/engine"
)

var (
	httpClient = utils.NewHTTPClient("mailroom")
)

// FireCampaignEvent starts the flow for the passed in org, contact and flow
func FireCampaignEvent(ctx context.Context, mr *mailroom.Mailroom, orgID models.OrgID, contactID flows.ContactID, flowUUID assets.FlowUUID, event *triggers.CampaignEvent, triggeredOn time.Time) (*models.Session, error) {
	// create our org assets
	org, err := models.NewOrgAssets(ctx, mr.DB, orgID)
	if err != nil {
		return nil, errors.Annotatef(err, "error creating assets for org: %d", orgID)
	}

	// try to load our flow
	flow, err := org.Flow(flowUUID)
	if err != nil {
		return nil, errors.Annotatef(err, "error loading campaign flow: %s", flowUUID)
	}

	// load our contact
	contacts, err := models.LoadContacts(ctx, mr.DB, org, []flows.ContactID{contactID})
	if err != nil {
		return nil, errors.Annotatef(err, "err loading contact: %d", contactID)
	}

	// create our trigger
	flowRef := flows.NewFlowReference(flow.UUID(), flow.Name())
	trigger := triggers.NewCampaignTrigger(org.Env(), flowRef, contacts[0], event, triggeredOn)

	// and start our flow
	session, err := StartFlow(ctx, mr, org, trigger)
	if err != nil {
		return nil, errors.Annotatef(err, "error starting flow for event: %s", event)
	}

	return session, nil
}

// StartFlow runs the passed in flow for the passed in contact
func StartFlow(ctx context.Context, mr *mailroom.Mailroom, org *models.OrgAssets, trigger flows.Trigger) (*models.Session, error) {
	// create our session
	// TODO: non default config for engine
	// TODO: fancier http client?
	assets := engine.NewSessionAssets(org)
	session := engine.NewSession(org, engine.NewDefaultConfig(), httpClient)

	// start our flow
	err := session.Start(trigger, nil)
	if err != nil {
		return nil, errors.Annotatef(err, "error starting flow: %s", trigger)
	}

	// write our session to the db
	tx, err := mr.DB.BeginTxx(ctx, nil)
	if err != nil {
		return nil, errors.Annotatef(err, "error starting transaction")
	}

	dbSession, err := models.WriteSession(ctx, tx, assets, session)
	if err != nil {
		tx.Rollback()
		return nil, errors.Annotatef(err, "error writing flow results for campaign: %s", trigger)
	}
	err = tx.Commit()
	if err != nil {
		tx.Rollback()
		return nil, errors.Annotatef(err, "error committing flow result write: %s", trigger)
	}

	// queue any messages created to courier
	rc := mr.RedisPool.Get()
	defer rc.Close()

	outbox := dbSession.GetOutbox()
	if len(outbox) > 0 {
		log := logrus.WithField("messages", dbSession.GetOutbox()).WithField("session", dbSession.ID)
		err := courier.QueueMessages(rc, outbox)

		// not being able to queue a message isn't the end of the world, log but don't return an error
		if err != nil {
			log.WithError(err).Error("error queuing message")
		}

		// update the status of the message in the db
		err = models.MarkMessagesQueued(ctx, mr.DB, outbox)
		if err != nil {
			log.WithError(err).Error("error marking message as queued")
		}
	}

	return dbSession, nil
}
