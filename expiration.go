package mailroom

import (
	"context"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
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

func startExpiring(mr *Mailroom) {
	// we run expiration every minute on the minute
	for true {
		wait := 61 - time.Now().Second()
		select {
		case <-mr.quit:
			// we are exiting, break out of our loop so our goroutine can exit
			break

		case <-time.After(time.Second * time.Duration(wait)):
			rc := mr.redisPool.Get()

			// try to insert our expiring lock to redis
			lockValue := cron.MakeKey(10)
			log := logrus.WithField("comp", "expirer").WithField("lock", lockValue)

			locked, err := cron.GrabLock(rc, expirationLock, lockValue, 900)
			if err != nil {
				log.WithError(err).Error("error acquiring lock")
				err := cron.ReleaseLock(rc, expirationLock, lockValue)
				if err != nil {
					log.WithError(err).Error("error releasing lock")
				}
				continue
			}

			if !locked {
				log.Info("lock already present, sleeping")
				continue
			}

			// ok, got the lock, go expire our runs
			err = expireRuns(mr, rc, lockValue)
			if err != nil {
				err := cron.ReleaseLock(rc, expirationLock, lockValue)
				if err != nil {
					log.WithError(err).Error("error releasing lock")
				}
			}

			rc.Close()
		}
	}
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
func expireRuns(mr *Mailroom, rc redis.Conn, lockValue string) error {
	log := logrus.WithField("comp", "expirer").WithField("lock", lockValue)

	// find all runs that need to be expired (we exclude IVR runs)
	runIDs := []flowRunRef{}
	ctx, cancel := context.WithTimeout(mr.ctx, time.Minute*5)
	defer cancel()

	err := mr.db.SelectContext(ctx, &runIDs, expiredRunQuery)
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
			err = cron.SetLockExpiraton(rc, expirationLock, lockValue, 400)
			if err != nil {
				log.WithError(err).Error("error setting lock expiration")
				return err
			}

			// expiration shouldn't take more than a few minutes
			ctx, cancel := context.WithTimeout(mr.ctx, time.Minute*5)
			defer cancel()

			// execute our query
			now := time.Now()
			sql, params, err := sqlx.In(expireRunsQuery, now, now, batchIDs)
			if err != nil {
				log.WithError(err).Error("error binding expiration query")
				return err
			}
			sql = mr.db.Rebind(sql)
			_, err = mr.db.ExecContext(ctx, sql, params...)
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

	// set our lock to run again in a minute
	wait := 60 - time.Now().Second()
	err = cron.SetLockExpiraton(rc, expirationLock, lockValue, wait)
	if err != nil {
		log.WithError(err).Error("error setting lock expiration")
	}
	return err
}
