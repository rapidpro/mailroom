package expirations

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/nyaruka/mailroom"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/tasks/handler"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/utils/cron"
	"github.com/nyaruka/redisx"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	expirationLock  = "run_expirations"
	expireBatchSize = 500
)

var expirationsMarker = redisx.NewIntervalSet("run_expirations", time.Hour*24, 2)

func init() {
	mailroom.AddInitFunction(StartExpirationCron)
}

// StartExpirationCron starts our cron job of expiring runs every minute
func StartExpirationCron(rt *runtime.Runtime, wg *sync.WaitGroup, quit chan bool) error {
	cron.Start(quit, rt, expirationLock, time.Second*60, false,
		func() error {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
			defer cancel()
			return HandleWaitExpirations(ctx, rt)
		},
	)
	return nil
}

// HandleWaitExpirations handles waiting sessions whose waits have expired, resuming those that can be resumed,
// and expiring those that can't
func HandleWaitExpirations(ctx context.Context, rt *runtime.Runtime) error {
	log := logrus.WithField("comp", "expirer")
	start := time.Now()

	rc := rt.RP.Get()
	defer rc.Close()

	// we expire sessions that can't be resumed in batches
	expiredSessions := make([]models.SessionID, 0, expireBatchSize)

	// select messaging sessions with expired waits
	rows, err := rt.DB.QueryxContext(ctx, sqlSelectExpiredWaits)
	if err != nil {
		return errors.Wrapf(err, "error querying for expired waits")
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		expiredWait := &ExpiredWait{}
		err := rows.StructScan(expiredWait)
		if err != nil {
			return errors.Wrapf(err, "error scanning expired wait")
		}

		count++

		// if it can't be resumed, add to batch to be expired
		if !expiredWait.CanResume {
			expiredSessions = append(expiredSessions, expiredWait.SessionID)

			// batch is full? commit it
			if len(expiredSessions) == expireBatchSize {
				err = models.ExitSessions(ctx, rt.DB, expiredSessions, models.SessionStatusExpired)
				if err != nil {
					return errors.Wrapf(err, "error expiring batch of sessions")
				}
				expiredSessions = expiredSessions[:0]
			}

			continue
		}

		// create a contact task to resume this session
		taskID := fmt.Sprintf("%d:%s", expiredWait.SessionID, expiredWait.ExpiresOn.Format(time.RFC3339))
		queued, err := expirationsMarker.Contains(rc, taskID)
		if err != nil {
			return errors.Wrapf(err, "error checking whether expiration is queued")
		}

		// already queued? move on
		if queued {
			continue
		}

		// ok, queue this task
		task := handler.NewExpirationTask(expiredWait.OrgID, expiredWait.ContactID, expiredWait.SessionID, expiredWait.ExpiresOn)
		err = handler.QueueHandleTask(rc, expiredWait.ContactID, task)
		if err != nil {
			return errors.Wrapf(err, "error adding new expiration task")
		}

		// and mark it as queued
		err = expirationsMarker.Add(rc, taskID)
		if err != nil {
			return errors.Wrapf(err, "error marking expiration task as queued")
		}
	}

	// commit any stragglers
	if len(expiredSessions) > 0 {
		err = models.ExitSessions(ctx, rt.DB, expiredSessions, models.SessionStatusExpired)
		if err != nil {
			return errors.Wrapf(err, "error expiring runs and sessions")
		}
	}

	log.WithField("elapsed", time.Since(start)).WithField("count", count).Info("expirations complete")
	return nil
}

const sqlSelectExpiredWaits = `
  SELECT id, org_id, contact_id, wait_expires_on, wait_resume_on_expire 
    FROM flows_flowsession
   WHERE session_type = 'M' AND status = 'W' AND wait_expires_on <= NOW()
ORDER BY wait_expires_on ASC
   LIMIT 25000`

type ExpiredWait struct {
	SessionID models.SessionID `db:"id"`
	OrgID     models.OrgID     `db:"org_id"`
	ContactID models.ContactID `db:"contact_id"`
	ExpiresOn time.Time        `db:"wait_expires_on"`
	CanResume bool             `db:"wait_resume_on_expire"`
}
