package ivr

import (
	"context"
	"time"

	"github.com/nyaruka/mailroom/config"
	"github.com/nyaruka/mailroom/models"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/mailroom"
	"github.com/nyaruka/mailroom/cron"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	ivrLock = "retry_ivr_calls"
)

func init() {
	mailroom.AddInitFunction(StartIVRCron)
}

// StartIVRCron starts our cron job of retrying errored calls
func StartIVRCron(mr *mailroom.Mailroom) error {
	cron.StartCron(mr.Quit, mr.RP, ivrLock, time.Minute,
		func(lockName string, lockValue string) error {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
			defer cancel()
			return retryErroredCalls(ctx, mr.Config, mr.DB, mr.RP, lockName, lockValue)
		},
	)

	return nil
}

// retryErroredCalls looks for all errored calls and retries them
func retryErroredCalls(ctx context.Context, config *config.Config, db *sqlx.DB, rp *redis.Pool, lockName string, lockValue string) error {
	log := logrus.WithField("comp", "ivr_cron").WithField("lock", lockValue)
	start := time.Now()

	// find all calls that need restarting
	ctx, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	conns, err := models.LoadChannelConnectionsToRetry(ctx, db, 100)
	if err != nil {
		return errors.Wrapf(err, "error loading connections to retry")
	}

	// schedules calls for each connection
	for _, conn := range conns {
		log = log.WithField("connection_id", conn.ID())

		// load the org for this connection
		org, err := models.GetOrgAssets(ctx, db, conn.OrgID())
		if err != nil {
			log.WithError(err).WithField("org_id", conn.OrgID()).Error("error loading org")
			continue
		}

		// and the associated channel
		channel := org.ChannelByID(conn.ChannelID())
		if channel == nil {
			log.WithField("channel_id", conn.ChannelID()).Error("unable to load channel")
			continue
		}

		// finally load the full URN
		urn, err := models.URNForID(ctx, db, org, conn.ContactURNID())
		if err != nil {
			log.WithError(err).WithField("urn_id", conn.ContactURNID()).Error("unable to load contact urn")
			continue
		}

		err = RequestCallStartForConnection(ctx, config, db, channel, urn, conn)
		if err != nil {
			log.WithError(err).Error(err)
			continue
		}
	}

	log.WithField("count", len(conns)).WithField("elapsed", time.Since(start)).Info("retried errored calls")

	return nil
}
