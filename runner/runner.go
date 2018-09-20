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

	// build our triggers
	flowRef := assets.NewFlowReference(flow.UUID(), flow.Name())
	ts := make([]flows.Trigger, 0, len(contacts))
	now := time.Now()
	for _, contact := range contacts {
		ts = append(ts, triggers.NewCampaignTrigger(org.Env(), flowRef, contact, event, now))
	}

	// start our contacts
	sessions, err := StartFlow(ctx, db, rp, org, sessionAssets, ts)
	if err != nil {
		logrus.WithField("contact_ids", contactIDs).WithError(err).Errorf("error starting flow for campaign event: %s", event)
	}

	return sessions, nil
}

// StartFlow runs the passed in flow for the passed in contact
func StartFlow(ctx context.Context, db *sqlx.DB, rp *redis.Pool, org *models.OrgAssets, assets flows.SessionAssets, tgs []flows.Trigger) ([]*models.Session, error) {
	track := models.NewTrack(ctx, db, rp, org)

	// for each trigger start the flow
	sessions := make([]flows.Session, 0, len(tgs))
	for _, trigger := range tgs {
		// create the session for this flow and run
		session := engine.NewSession(assets, engine.NewDefaultConfig(), httpClient)

		// start our flow
		err := session.Start(trigger, nil)
		if err != nil {
			logrus.WithField("contact_id", trigger.Contact().ID()).WithError(err).Errorf("error starting flow: %s", trigger.Flow().UUID)
			continue
		}

		sessions = append(sessions, session)
	}

	// we write our sessions and all their objects in a single transaction
	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, errors.Annotatef(err, "error starting transaction")
	}

	// write our session to the db
	dbSessions, err := models.WriteSessions(ctx, tx, track, sessions)
	if err != nil {
		return nil, errors.Annotatef(err, "error writing sessions")
	}

	// commit it at once
	err = tx.Commit()

	// this was an error and this was a single session being committed, no use retrying
	if err != nil && len(sessions) == 1 {
		logrus.WithField("contact_id", sessions[0].Contact().ID()).WithError(err).Errorf("error writing session to db")
		return nil, errors.Annotatef(err, "error committing session")
	}

	// otherwise, it may have been just one session that killed us, retry them one at a time
	if err != nil {
		tx.Rollback()

		// we failed writing our sessions in one go, try one at a time
		tx, err := db.BeginTxx(ctx, nil)
		if err != nil {
			return nil, errors.Annotatef(err, "error starting transaction for retry")
		}
		for _, session := range sessions {
			dbSession, err := models.WriteSessions(ctx, tx, track, []flows.Session{session})
			if err != nil {
				logrus.WithField("contact_id", session.Contact().ID()).WithError(err).Errorf("error writing session to db")
				continue
			}

			err = tx.Commit()
			if err != nil {
				logrus.WithField("contact_id", session.Contact().ID()).WithError(err).Errorf("error comitting session to db")
				continue
			}

			dbSessions = append(dbSessions, dbSession[0])
		}
	}

	// queue any messages created to courier
	rc := rp.Get()
	defer rc.Close()

	for _, dbSession := range dbSessions {
		outbox := dbSession.Outbox()
		if len(outbox) > 0 {
			log := logrus.WithField("messages", dbSession.Outbox()).WithField("session", dbSession.ID)
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
	}

	return dbSessions, nil
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
