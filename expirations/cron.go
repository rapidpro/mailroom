package expirations

import (
	"context"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/mailroom"
	"github.com/nyaruka/mailroom/celery"
	"github.com/nyaruka/mailroom/cron"
	"github.com/sirupsen/logrus"
)

const (
	expirationLock    = "run_expirations"
	expireBatchSize   = 500
	continueTaskName  = "continue_parent_flows"
	continueTaskQueue = "celery"
)

// TODO: ideally this will never ship and we will handle expirations internally

func init() {
	// mailroom.AddInitFunction(StartExpirationCron)
}

// StartExpirationCron starts our cron job of expiring runs every minute
func StartExpirationCron(mr *mailroom.Mailroom) error {
	cron.StartCron(mr.Quit, mr.RedisPool, expirationLock, time.Second*60,
		func(lockName string, lockValue string) error {
			return expireRuns(mr, lockName, lockValue)
		},
	)
	return nil
}

const expiredRunQuery = `
	SELECT id, parent_id
	FROM flows_flowrun
	WHERE is_active = TRUE AND expires_on < NOW() AND connection_id IS NULL
	ORDER BY expires_on
	LIMIT 25000
`

const expireRunsQuery = `
	UPDATE flows_flowrun
	SET is_active = FALSE, exited_on = ?, exit_type = 'E', modified_on = ?, child_context = NULL, parent_context = NULL
	WHERE id IN (?)
`

type flowRunRef struct {
	ID       int64  `db:"id"`
	ParentID *int64 `db:"parent_id"`
}

// helper method to safely execute an IN query in the passed in transaction
func executeInQuery(ctx context.Context, db *sqlx.DB, query string, ids []int64) error {
	q, vs, err := sqlx.In(query, ids)
	if err != nil {
		return err
	}
	q = db.Rebind(q)

	_, err = db.ExecContext(ctx, q, vs...)
	return err
}

// expireRuns expires all the runs that have an expiration in the past
func expireRuns(mr *mailroom.Mailroom, lockName string, lockValue string) error {
	log := logrus.WithField("comp", "expirer").WithField("lock", lockValue)
	rc := mr.RedisPool.Get()
	defer rc.Close()

	// find all runs that need to be expired (we exclude IVR runs)
	runIDs := []flowRunRef{}
	ctx, cancel := context.WithTimeout(mr.CTX, time.Minute*5)
	defer cancel()

	err := mr.DB.SelectContext(ctx, &runIDs, expiredRunQuery)
	if err != nil {
		log.WithError(err).Error("error looking up runs to expire")
		return err
	}

	// expire them in our db
	log.WithField("count", len(runIDs)).Info("expiring runs")
	start := time.Now()

	// in groups of 500, expire these runs
	batchIDs := make([]int64, 0, expireBatchSize)
	continueIDs := make([]int64, 0, expireBatchSize)

	// expire each of our runs
	for i := 0; i < len(runIDs); i++ {
		batchIDs = append(batchIDs, runIDs[i].ID)
		if runIDs[i].ParentID != nil {
			continueIDs = append(continueIDs, *runIDs[i].ParentID)
		}

		// batch size or last element? expire the runs
		if i == len(runIDs)-1 || len(batchIDs) == expireBatchSize {
			// extend our timeout
			err = cron.ExtendLock(rc, lockName, lockValue, 60*6)
			if err != nil {
				log.WithError(err).Error("error setting lock expiration")
				return err
			}

			// expiration shouldn't take more than a few minutes
			ctx, cancel := context.WithTimeout(mr.CTX, time.Minute*5)
			defer cancel()

			// execute our query
			now := time.Now()
			sql, params, err := sqlx.In(expireRunsQuery, now, now, batchIDs)
			if err != nil {
				log.WithError(err).Error("error binding expiration query")
				return err
			}
			sql = mr.DB.Rebind(sql)
			_, err = mr.DB.ExecContext(ctx, sql, params...)
			if err != nil {
				log.WithField("runs", batchIDs).WithError(err).Error("error expiring batch of runs")
				return err
			}

			// if we have any runs that need to be continued, do so
			if len(continueIDs) > 0 {
				err := celery.QueueTask(rc, continueTaskQueue, continueTaskName, continueIDs)
				if err != nil {
					log.WithField("runs", continueIDs).WithError(err).Error("error queuing continuation of runs")
				}
				return err
			}

			batchIDs = batchIDs[:0]
			continueIDs = continueIDs[:0]
		}
	}

	log.WithField("elapsed", time.Since(start)).WithField("count", len(runIDs)).Info("expirations complete")
	return nil
}
