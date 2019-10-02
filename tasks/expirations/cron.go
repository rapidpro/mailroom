package expirations

import (
	"context"
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom"
	"github.com/nyaruka/mailroom/cron"
	"github.com/nyaruka/mailroom/tasks/handler"
	"github.com/nyaruka/mailroom/marker"
	"github.com/nyaruka/mailroom/models"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	expirationLock  = "run_expirations"
	markerGroup     = "run_expirations"
	expireBatchSize = 500
)

func init() {
	mailroom.AddInitFunction(StartExpirationCron)
}

// StartExpirationCron starts our cron job of expiring runs every minute
func StartExpirationCron(mr *mailroom.Mailroom) error {
	cron.StartCron(mr.Quit, mr.RP, expirationLock, time.Second*60,
		func(lockName string, lockValue string) error {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
			defer cancel()
			return expireRuns(ctx, mr.DB, mr.RP, lockName, lockValue)
		},
	)
	return nil
}

// expireRuns expires all the runs that have an expiration in the past
func expireRuns(ctx context.Context, db *sqlx.DB, rp *redis.Pool, lockName string, lockValue string) error {
	log := logrus.WithField("comp", "expirer").WithField("lock", lockValue)
	start := time.Now()

	rc := rp.Get()
	defer rc.Close()

	// we expire runs and sessions that have no continuation in batches
	expiredRuns := make([]models.FlowRunID, 0, expireBatchSize)
	expiredSessions := make([]models.SessionID, 0, expireBatchSize)

	// select our expired runs
	rows, err := db.QueryxContext(ctx, selectExpiredRunsSQL)
	if err != nil {
		return errors.Wrapf(err, "error querying for expired runs")
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		expiration := &RunExpiration{}
		err := rows.StructScan(expiration)
		if err != nil {
			return errors.Wrapf(err, "error scanning expired run")
		}

		count++

		// no parent id? we can add this to our batch
		if expiration.ParentUUID == nil {
			expiredRuns = append(expiredRuns, expiration.RunID)
			expiredSessions = append(expiredSessions, expiration.SessionID)

			// batch is full? commit it
			if len(expiredRuns) == expireBatchSize {
				err = models.ExpireRunsAndSessions(ctx, db, expiredRuns, expiredSessions)
				if err != nil {
					return errors.Wrapf(err, "error expiring runs and sessions")
				}
				expiredRuns = expiredRuns[:0]
				expiredSessions = expiredSessions[:0]
			}

			continue
		}

		// need to continue this session and flow, create a task for that
		taskID := fmt.Sprintf("%d:%s", expiration.RunID, expiration.ExpiresOn.Format(time.RFC3339))
		queued, err := marker.HasTask(rc, markerGroup, taskID)
		if err != nil {
			return errors.Wrapf(err, "error checking whether expiration is queued")
		}

		// already queued? move on
		if queued {
			continue
		}

		// ok, queue this task
		task := handler.NewExpirationTask(expiration.OrgID, expiration.ContactID, expiration.SessionID, expiration.RunID, expiration.ExpiresOn)
		err = handler.AddHandleTask(rc, expiration.ContactID, task)
		if err != nil {
			return errors.Wrapf(err, "error adding new expiration task")
		}

		// and mark it as queued
		err = marker.AddTask(rc, markerGroup, taskID)
		if err != nil {
			return errors.Wrapf(err, "error marking expiration task as queued")
		}
	}

	// commit any stragglers
	if len(expiredRuns) > 0 {
		err = models.ExpireRunsAndSessions(ctx, db, expiredRuns, expiredSessions)
		if err != nil {
			return errors.Wrapf(err, "error expiring runs and sessions")
		}
	}

	log.WithField("elapsed", time.Since(start)).WithField("count", count).Info("expirations complete")
	return nil
}

const selectExpiredRunsSQL = `
	SELECT
		fr.org_id as org_id,
		fr.flow_id as flow_id,
		fr.contact_id as contact_id,
		fr.id as run_id,
		fr.parent_uuid as parent_uuid,
		fr.session_id as session_id,
		fr.expires_on as expires_on
	FROM
		flows_flowrun fr
		JOIN orgs_org o ON fr.org_id = o.id
	WHERE
		fr.is_active = TRUE AND
		fr.expires_on < NOW() AND
		fr.connection_id IS NULL AND
		fr.session_id IS NOT NULL
	ORDER BY
		expires_on ASC
	LIMIT 25000
`

type RunExpiration struct {
	OrgID      models.OrgID     `db:"org_id"`
	FlowID     models.FlowID    `db:"flow_id"`
	ContactID  models.ContactID `db:"contact_id"`
	RunID      models.FlowRunID `db:"run_id"`
	ParentUUID *flows.RunUUID   `db:"parent_uuid"`
	SessionID  models.SessionID `db:"session_id"`
	ExpiresOn  time.Time        `db:"expires_on"`
}
