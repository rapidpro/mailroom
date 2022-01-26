package ivr

import (
	"context"
	"sync"
	"time"

	"github.com/nyaruka/mailroom"
	"github.com/nyaruka/mailroom/core/ivr"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/utils/cron"
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
func StartIVRCron(rt *runtime.Runtime, wg *sync.WaitGroup, quit chan bool) error {
	cron.Start(quit, rt, retryIVRLock, time.Minute, false,
		func() error {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
			defer cancel()
			return RetryCalls(ctx, rt)
		},
	)

	cron.Start(quit, rt, expireIVRLock, time.Minute, false,
		func() error {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
			defer cancel()
			return ExpireVoiceSessions(ctx, rt)
		},
	)

	return nil
}

// RetryCalls looks for calls that need to be retried and retries them
func RetryCalls(ctx context.Context, rt *runtime.Runtime) error {
	log := logrus.WithField("comp", "ivr_cron_retryer")
	start := time.Now()

	// find all calls that need restarting
	ctx, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	conns, err := models.LoadChannelConnectionsToRetry(ctx, rt.DB, 100)
	if err != nil {
		return errors.Wrapf(err, "error loading connections to retry")
	}

	throttledChannels := make(map[models.ChannelID]bool)

	// schedules calls for each connection
	for _, conn := range conns {
		log = log.WithField("connection_id", conn.ID())

		// if the channel for this connection is throttled, move on
		if throttledChannels[conn.ChannelID()] {
			conn.MarkThrottled(ctx, rt.DB, time.Now())
			log.WithField("channel_id", conn.ChannelID()).Info("skipping connection, throttled")
			continue
		}

		// load the org for this connection
		oa, err := models.GetOrgAssets(ctx, rt, conn.OrgID())
		if err != nil {
			log.WithError(err).WithField("org_id", conn.OrgID()).Error("error loading org")
			continue
		}

		// and the associated channel
		channel := oa.ChannelByID(conn.ChannelID())
		if channel == nil {
			// fail this call, channel is no longer active
			err = models.UpdateChannelConnectionStatuses(ctx, rt.DB, []models.ConnectionID{conn.ID()}, models.ConnectionStatusFailed)
			if err != nil {
				log.WithError(err).WithField("channel_id", conn.ChannelID()).Error("error marking call as failed due to missing channel")
			}
			continue
		}

		// finally load the full URN
		urn, err := models.URNForID(ctx, rt.DB, oa, conn.ContactURNID())
		if err != nil {
			log.WithError(err).WithField("urn_id", conn.ContactURNID()).Error("unable to load contact urn")
			continue
		}

		err = ivr.RequestCallStartForConnection(ctx, rt, channel, urn, conn)
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

// ExpireVoiceSessions looks for voice sessions that should be expired and ends them
func ExpireVoiceSessions(ctx context.Context, rt *runtime.Runtime) error {
	log := logrus.WithField("comp", "ivr_cron_expirer")
	start := time.Now()

	ctx, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	// select voice sessions with expired waits
	rows, err := rt.DB.QueryxContext(ctx, sqlSelectExpiredWaits)
	if err != nil {
		return errors.Wrapf(err, "error querying for expired waits")
	}
	defer rows.Close()

	expiredSessions := make([]models.SessionID, 0, 100)

	for rows.Next() {
		expiredWait := &ExpiredWait{}
		err := rows.StructScan(expiredWait)
		if err != nil {
			return errors.Wrapf(err, "error scanning expired wait")
		}

		// add the session to those we need to expire
		expiredSessions = append(expiredSessions, expiredWait.SessionID)

		// load our connection
		conn, err := models.SelectChannelConnection(ctx, rt.DB, expiredWait.ConnectionID)
		if err != nil {
			log.WithError(err).WithField("connection_id", expiredWait.ConnectionID).Error("unable to load connection")
			continue
		}

		// hang up our call
		err = ivr.HangupCall(ctx, rt, conn)
		if err != nil {
			log.WithError(err).WithField("connection_id", conn.ID()).Error("error hanging up call")
		}
	}

	// now expire our runs and sessions
	if len(expiredSessions) > 0 {
		err := models.ExitSessions(ctx, rt.DB, expiredSessions, models.SessionStatusExpired)
		if err != nil {
			log.WithError(err).Error("error expiring sessions for expired calls")
		}
		log.WithField("count", len(expiredSessions)).WithField("elapsed", time.Since(start)).Info("expired and hung up on channel connections")
	}

	return nil
}

const sqlSelectExpiredWaits = `
  SELECT id, connection_id, wait_expires_on
    FROM flows_flowsession
   WHERE session_type = 'V' AND status = 'W' AND wait_expires_on <= NOW()
ORDER BY wait_expires_on ASC
   LIMIT 100`

type ExpiredWait struct {
	SessionID    models.SessionID    `db:"id"`
	ConnectionID models.ConnectionID `db:"connection_id"`
	ExpiresOn    time.Time           `db:"wait_expires_on"`
}
