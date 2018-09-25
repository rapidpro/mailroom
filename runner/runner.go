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
	"github.com/nyaruka/librato"
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

	start := time.Now()

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

	// log both our total and average
	librato.Gauge("mr.campaign_event_elapsed", float64(time.Since(start))/float64(time.Second))
	librato.Gauge("mr.campaign_event_count", float64(len(sessions)))

	return sessions, nil
}

// StartFlow runs the passed in flow for the passed in contact
func StartFlow(ctx context.Context, db *sqlx.DB, rp *redis.Pool, org *models.OrgAssets, assets flows.SessionAssets, tgs []flows.Trigger) ([]*models.Session, error) {
	if len(tgs) == 0 {
		return nil, nil
	}

	start := time.Now()
	log := logrus.WithField("flow_name", tgs[0].Flow().Name).WithField("flow_uuid", tgs[0].Flow().UUID)

	// for each trigger start the flow
	sessions := make([]flows.Session, 0, len(tgs))
	for _, trigger := range tgs {
		// create the session for this flow and run
		session := engine.NewSession(assets, engine.NewDefaultConfig(), httpClient)

		// start our flow
		log := log.WithField("contact_uuid", trigger.Contact().UUID())
		start := time.Now()
		err := session.Start(trigger, nil)
		if err != nil {
			log.WithError(err).Errorf("error starting flow")
			continue
		}
		log.WithField("elapsed", time.Since(start)).Info("flow engine start")
		librato.Gauge("mr.flow_start_elapsed", float64(time.Since(start)))

		sessions = append(sessions, session)
	}

	// we write our sessions and all their objects in a single transaction
	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, errors.Annotatef(err, "error starting transaction")
	}

	// write our session to the db
	dbSessions, err := models.WriteSessions(ctx, tx, rp, org, sessions)
	if err != nil {
		return nil, errors.Annotatef(err, "error writing sessions")
	}

	// commit it at once
	err = tx.Commit()

	// this was an error and this was a single session being committed, no use retrying
	if err != nil && len(sessions) == 1 {
		log.WithField("contact_uuid", sessions[0].Contact().UUID()).WithError(err).Errorf("error writing session to db")
		return nil, errors.Annotatef(err, "error committing session")
	}

	// otherwise, it may have been just one session that killed us, retry them one at a time
	if err != nil {
		tx.Rollback()

		// we failed writing our sessions in one go, try one at a time
		for _, session := range sessions {
			tx, err := db.BeginTxx(ctx, nil)
			if err != nil {
				return nil, errors.Annotatef(err, "error starting transaction for retry")
			}

			dbSession, err := models.WriteSessions(ctx, tx, rp, org, []flows.Session{session})
			if err != nil {
				tx.Rollback()
				log.WithField("contact_uuid", session.Contact().UUID()).WithError(err).Errorf("error writing session to db")
				continue
			}

			err = tx.Commit()
			if err != nil {
				tx.Rollback()
				log.WithField("contact_uuid", session.Contact().UUID()).WithError(err).Errorf("error comitting session to db")
				continue
			}

			dbSessions = append(dbSessions, dbSession[0])
		}
	}

	// now take care of any post-commit hooks
	tx, err = db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, errors.Annotatef(err, "error starting transaction for post commit hooks")
	}

	err = models.ApplyPostEventHooks(ctx, tx, rp, org.OrgID(), dbSessions)
	if err == nil {
		err = tx.Commit()
	}
	if err != nil {
		tx.Rollback()

		// we failed with our post commit hooks, try one at a time, logging those errors
		for _, session := range dbSessions {
			log = log.WithField("contact_uuid", session.ContactUUID())

			tx, err := db.BeginTxx(ctx, nil)
			if err != nil {
				tx.Rollback()
				log.WithError(err).Error("error starting transaction to retry post commits")
				continue
			}

			err = models.ApplyPostEventHooks(ctx, tx, rp, org.OrgID(), []*models.Session{session})
			if err != nil {
				tx.Rollback()
				log.WithError(err).Errorf("error applying post commit hook")
				continue
			}

			err = tx.Commit()
			if err != nil {
				tx.Rollback()
				log.WithError(err).Errorf("error comitting post commit hook")
				continue
			}
		}
	}

	// figure out both average and total for total execution and commit time for our flows
	librato.Gauge("mr.flow_start_elapsed", float64(time.Since(start))/float64(time.Second))
	librato.Gauge("mr.flow_start_count", float64(len(dbSessions)))
	log.WithField("elapsed", time.Since(start)).WithField("count", len(dbSessions)).Info("flows started, sessions created")

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
