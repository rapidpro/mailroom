package runner

import (
	"context"
	"time"

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
func FireCampaignEvent(mr *mailroom.Mailroom, orgID models.OrgID, contactID models.ContactID, flowUUID flows.FlowUUID, event *triggers.CampaignEvent, triggeredOn time.Time) (*models.Session, error) {
	// campaign fires shouldn't take longer than a minute
	ctx, cancel := context.WithTimeout(mr.CTX, time.Minute)
	defer cancel()

	// grab our org
	org := models.NewOrgAssets(ctx, mr.DB, orgID)

	// TODO: load appropriate environment for org (should probably be method on OrgAssets)
	env := utils.NewDefaultEnvironment()

	// try to load our flow
	flow, err := org.GetFlow(flowUUID)
	if err != nil {
		logrus.WithError(err).Error("error loading flow")
		return nil, err
	}

	// TODO: get a lock for the contact so that nobody else is running the contact in a flow
	contact, err := models.LoadContact(org, contactID)
	if err != nil {
		logrus.WithError(err).Error("error loading contact")
		return nil, err
	}

	// create our trigger
	trigger := triggers.NewCampaignTrigger(env, flow, contact, event, triggeredOn)
	return StartFlow(ctx, mr, org, trigger)
}

// StartFlow runs the passed in flow for the passed in contact
func StartFlow(ctx context.Context, mr *mailroom.Mailroom, assets *models.OrgAssets, trigger flows.Trigger) (*models.Session, error) {
	// create our session
	// TODO: non default config for engine
	// TODO: fancier http client?
	session := engine.NewSession(assets, engine.NewDefaultConfig(), httpClient)

	// start our flow
	err := session.Start(trigger, nil)
	if err != nil {
		logrus.WithError(err).Error("error starting flow")
		return nil, err
	}

	// write our session to the db
	tx, err := mr.DB.BeginTxx(ctx, nil)
	if err != nil {
		return nil, err
	}

	dbSession, err := models.CreateSession(ctx, tx, assets, session)
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	err = tx.Commit()
	if err != nil {
		tx.Rollback()
		return nil, err
	}

	// queue any messages created
	rc := mr.RedisPool.Get()
	defer rc.Close()

	outbox := dbSession.GetOutbox()
	if len(outbox) > 0 {
		err := courier.QueueMessages(rc, outbox)

		// not being able to queue a message isn't the end of the world, log but don't return an error
		if err != nil {
			logrus.WithError(err).Error("error queuing message")
		}

		// update the status of the message in the db
		// TODO: we should be able to do this all in one go really
		// TODO: we should be queuing all messages in one insert
		err = models.MarkMessagesQueued(ctx, mr.DB, outbox)
		if err != nil {
			logrus.WithError(err).Error("error marking message as queued")
		}
	}

	return dbSession, nil
}
