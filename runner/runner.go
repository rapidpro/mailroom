package runner

import (
	"context"
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/juju/errors"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows/triggers"
	"github.com/nyaruka/mailroom/courier"
	"github.com/nyaruka/mailroom/models"
	cache "github.com/patrickmn/go-cache"
	"github.com/sirupsen/logrus"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/utils"

	"github.com/nyaruka/goflow/flows/engine"
)

var (
	httpClient = utils.NewHTTPClient("mailroom")
	assetCache = cache.New(5*time.Second, time.Minute)
)

// FireCampaignEvent starts the flow for the passed in org, contact and flow
func FireCampaignEvent(
	ctx context.Context, db *sqlx.DB, rp *redis.Pool,
	orgID models.OrgID, contactIDs []flows.ContactID, flowUUID assets.FlowUUID,
	event *triggers.CampaignEvent) ([]*models.Session, error) {

	// create our org assets
	org, err := models.GetOrgAssets(ctx, db, orgID)
	if err != nil {
		return nil, errors.Annotatef(err, "error creating assets for org: %d", orgID)
	}

	// try to load our flow
	flow, err := org.Flow(flowUUID)
	if err != nil {
		return nil, errors.Annotatef(err, "error loading campaign flow: %s", flowUUID)
	}

	// create our assets
	sessionAssets, err := getSessionAssets(org)
	if err != nil {
		return nil, errors.Annotatef(err, "err creating session assets for org: %d", org.OrgID())
	}

	// load our contacts
	contacts, err := models.LoadContacts(ctx, db, sessionAssets, org, contactIDs)
	if err != nil {
		return nil, errors.Annotatef(err, "err loading contacts: %v", contactIDs)
	}

	sessions := make([]*models.Session, 0, len(contacts))

	// for each of our contacts
	for _, contact := range contacts {
		// create our trigger
		flowRef := assets.NewFlowReference(flow.UUID(), flow.Name())
		trigger := triggers.NewCampaignTrigger(org.Env(), flowRef, contact, event, time.Now())

		// and start our flow
		session, err := StartFlow(ctx, db, rp, org, sessionAssets, trigger)
		if err != nil {
			logrus.WithField("contact_id", contact.ID()).WithError(err).Errorf("error starting flow for event: %s", event)
			continue
		}

		sessions = append(sessions, session)
	}

	return sessions, nil
}

// StartFlow runs the passed in flow for the passed in contact
func StartFlow(ctx context.Context, db *sqlx.DB, rp *redis.Pool, org *models.OrgAssets, assets flows.SessionAssets, trigger flows.Trigger) (*models.Session, error) {
	// create the session for this flow and run
	session := engine.NewSession(assets, engine.NewDefaultConfig(), httpClient)
	track := models.NewTrack(ctx, db, rp, org)

	// start our flow
	err := session.Start(trigger, nil)
	if err != nil {
		return nil, errors.Annotatef(err, "error starting flow: %s", trigger)
	}

	// we write our session in a single transaction
	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, errors.Annotatef(err, "error starting transaction")
	}

	// write our session to the db
	dbSession, err := models.WriteSession(ctx, tx, track, session)
	if err != nil {
		tx.Rollback()
		return nil, errors.Annotatef(err, "error writing flow results for campaign: %s", trigger)
	}

	// commit it at once, this will create our messages
	err = tx.Commit()
	if err != nil {
		tx.Rollback()
		return nil, errors.Annotatef(err, "error committing flow result write: %s", trigger)
	}

	// queue any messages created to courier
	rc := rp.Get()
	defer rc.Close()

	outbox := dbSession.GetOutbox()
	if len(outbox) > 0 {
		log := logrus.WithField("messages", dbSession.GetOutbox()).WithField("session", dbSession.ID)
		err := courier.QueueMessages(rc, outbox)

		// not being able to queue a message isn't the end of the world, log but don't return an error
		if err != nil {
			log.WithError(err).Error("error queuing message")

			// in the case of errors we do want to change the messages back to pending however so they
			// get queued later. (for the common case messages are only inserted and queued, without a status update)
			err = models.MarkMessagesPending(ctx, db, outbox)
			if err != nil {
				log.WithError(err).Error("error marking message as pending")
			}
		}
	}

	return dbSession, nil
}

func getSessionAssets(org *models.OrgAssets) (flows.SessionAssets, error) {
	key := fmt.Sprintf("%d", org.OrgID())
	cached, found := assetCache.Get(key)
	if found {
		return cached.(flows.SessionAssets), nil
	}

	assets, err := engine.NewSessionAssets(org)
	if err != nil {
		return nil, err
	}

	assetCache.Set(key, assets, cache.DefaultExpiration)
	return assets, nil
}
