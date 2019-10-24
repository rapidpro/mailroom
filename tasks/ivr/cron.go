package ivr

import (
	"context"
	"time"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/config"
	"github.com/nyaruka/mailroom/ivr"
	"github.com/nyaruka/mailroom/models"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/mailroom"
	"github.com/nyaruka/mailroom/cron"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	retryIVRLock  = "retry_ivr_calls"
	expireIVRLock = "expire_ivr_calls"
)

func init() {
	mailroom.AddInitFunction(StartIVRCron)
}

// StartIVRCron starts our cron job of retrying errored calls
func StartIVRCron(mr *mailroom.Mailroom) error {
	cron.StartCron(mr.Quit, mr.RP, retryIVRLock, time.Minute,
		func(lockName string, lockValue string) error {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
			defer cancel()
			return retryCalls(ctx, mr.Config, mr.DB, mr.RP, retryIVRLock, lockValue)
		},
	)

	cron.StartCron(mr.Quit, mr.RP, expireIVRLock, time.Minute,
		func(lockName string, lockValue string) error {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
			defer cancel()
			return expireCalls(ctx, mr.Config, mr.DB, mr.RP, expireIVRLock, lockValue)
		},
	)

	return nil
}

// retryCalls looks for calls that need to be retried and retries them
func retryCalls(ctx context.Context, config *config.Config, db *sqlx.DB, rp *redis.Pool, lockName string, lockValue string) error {
	log := logrus.WithField("comp", "ivr_cron_retryer").WithField("lock", lockValue)
	start := time.Now()

	// find all calls that need restarting
	ctx, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	conns, err := models.LoadChannelConnectionsToRetry(ctx, db, 100)
	if err != nil {
		return errors.Wrapf(err, "error loading connections to retry")
	}

	throttledChannels := make(map[models.ChannelID]bool)

	// schedules calls for each connection
	for _, conn := range conns {
		log = log.WithField("connection_id", conn.ID())

		// if the channel for this connection is throttled, move on
		if throttledChannels[conn.ChannelID()] {
			conn.MarkThrottled(ctx, db, time.Now())
			log.WithField("channel_id", conn.ChannelID()).Info("skipping connection, throttled")
			continue
		}

		// load the org for this connection
		org, err := models.GetOrgAssets(ctx, db, conn.OrgID())
		if err != nil {
			log.WithError(err).WithField("org_id", conn.OrgID()).Error("error loading org")
			continue
		}

		// and the associated channel
		channel := org.ChannelByID(conn.ChannelID())
		if channel == nil {
			// fail this call, channel is no longer active
			err = models.UpdateChannelConnectionStatuses(ctx, db, []models.ConnectionID{conn.ID()}, models.ConnectionStatusFailed)
			if err != nil {
				log.WithError(err).WithField("channel_id", conn.ChannelID()).Error("error marking call as failed due to missing channel")
			}
			continue
		}

		// finally load the full URN
		urn, err := models.URNForID(ctx, db, org, conn.ContactURNID())
		if err != nil {
			log.WithError(err).WithField("urn_id", conn.ContactURNID()).Error("unable to load contact urn")
			continue
		}

		err = ivr.RequestCallStartForConnection(ctx, config, db, channel, urn, conn)
		if err != nil {
			log.WithError(err).Error(err)
			continue
		}

		// queued status on a connection we just tried means it is throttled, mark our channel as such
		throttledChannels[conn.ChannelID()] = true
	}

	log.WithField("count", len(conns)).WithField("elapsed", time.Since(start)).Info("retried errored calls")

	return nil
}

// expireCalls looks for calls that should be expired and ends them
func expireCalls(ctx context.Context, config *config.Config, db *sqlx.DB, rp *redis.Pool, lockName string, lockValue string) error {
	log := logrus.WithField("comp", "ivr_cron_expirer").WithField("lock", lockValue)
	start := time.Now()

	ctx, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	// select our expired runs
	rows, err := db.QueryxContext(ctx, selectExpiredRunsSQL)
	if err != nil {
		return errors.Wrapf(err, "error querying for expired runs")
	}
	defer rows.Close()

	expiredRuns := make([]models.FlowRunID, 0, 100)
	expiredSessions := make([]models.SessionID, 0, 100)

	for rows.Next() {
		exp := &RunExpiration{}
		err := rows.StructScan(exp)
		if err != nil {
			return errors.Wrapf(err, "error scanning expired run")
		}

		// add the run and session to those we need to expire
		expiredRuns = append(expiredRuns, exp.RunID)
		expiredSessions = append(expiredSessions, exp.SessionID)

		// load our connection
		conn, err := models.SelectChannelConnection(ctx, db, exp.ConnectionID)
		if err != nil {
			log.WithError(err).WithField("connection_id", exp.ConnectionID).Error("unable to load connection")
			continue
		}

		// hang up our call
		err = ivr.HangupCall(ctx, config, db, conn)
		if err != nil {
			log.WithError(err).WithField("connection_id", conn.ID()).Error("error hanging up call")
		}
	}

	// now expire our runs and sessions
	if len(expiredRuns) > 0 {
		err := models.ExpireRunsAndSessions(ctx, db, expiredRuns, expiredSessions)
		if err != nil {
			log.WithError(err).Error("error expiring runs and sessions for expired calls")
		}
		log.WithField("count", len(expiredRuns)).WithField("elapsed", time.Since(start)).Info("expired and hung up on channel connections")
	}

	return nil
}

const selectExpiredRunsSQL = `
	SELECT
		fr.id as run_id,	
		fr.org_id as org_id,
		fr.flow_id as flow_id,
		fr.contact_id as contact_id,
		fr.session_id as session_id,
		fr.expires_on as expires_on,
		cc.id as connection_id
	FROM
		flows_flowrun fr
		JOIN orgs_org o ON fr.org_id = o.id
		JOIN channels_channelconnection cc ON fr.connection_id = cc.id
	WHERE
		fr.is_active = TRUE AND
		fr.expires_on < NOW() AND
		fr.connection_id IS NOT NULL AND
		fr.session_id IS NOT NULL AND
        cc.connection_type = 'V'
	ORDER BY
		expires_on ASC
	LIMIT 100
`

type RunExpiration struct {
	OrgID        models.OrgID        `db:"org_id"`
	FlowID       models.FlowID       `db:"flow_id"`
	ContactID    flows.ContactID     `db:"contact_id"`
	RunID        models.FlowRunID    `db:"run_id"`
	SessionID    models.SessionID    `db:"session_id"`
	ExpiresOn    time.Time           `db:"expires_on"`
	ConnectionID models.ConnectionID `db:"connection_id"`
}
