package timeouts

import (
	"context"
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/juju/errors"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom"
	"github.com/nyaruka/mailroom/cron"
	"github.com/nyaruka/mailroom/handler"
	"github.com/nyaruka/mailroom/marker"
	"github.com/nyaruka/mailroom/models"
	"github.com/sirupsen/logrus"
)

const (
	timeoutLock = "run_timeouts"
	markerGroup = "run_timeouts"
)

func init() {
	mailroom.AddInitFunction(StartTimeoutCron)
}

// StartTimeoutCron starts our cron job of continuing timed out sessions every minute
func StartTimeoutCron(mr *mailroom.Mailroom) error {
	cron.StartCron(mr.Quit, mr.RP, timeoutLock, time.Second*60,
		func(lockName string, lockValue string) error {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
			defer cancel()
			return timeoutRuns(ctx, mr.DB, mr.RP, lockName, lockValue)
		},
	)
	return nil
}

// timeoutRuns looks for any runs that have timed out and schedules for them to continue
func timeoutRuns(ctx context.Context, db *sqlx.DB, rp *redis.Pool, lockName string, lockValue string) error {
	log := logrus.WithField("comp", "timeout").WithField("lock", lockValue)
	start := time.Now()

	// find all runs that need to be expired (we exclude IVR runs)
	rows, err := db.QueryxContext(ctx, timedoutRunsSQL)
	if err != nil {
		return errors.Annotatef(err, "error selecting timed out runs")
	}
	defer rows.Close()

	rc := rp.Get()
	defer rc.Close()

	// add a timeout task for each run
	count := 0
	timeout := &Timeout{}
	for rows.Next() {
		err := rows.StructScan(timeout)
		if err != nil {
			return errors.Annotatef(err, "error scanning timeout")
		}

		// check whether we've already queued this
		taskID := fmt.Sprintf("%d:%s", timeout.RunID, timeout.TimeoutOn.Format(time.RFC3339))
		queued, err := marker.HasTask(rc, markerGroup, taskID)
		if err != nil {
			return errors.Annotatef(err, "error checking whether task is queued")
		}

		// already queued? move on
		if queued {
			continue
		}

		// ok, queue this task
		task := handler.NewTimeoutEvent(timeout.OrgID, timeout.ContactID, timeout.FlowID, timeout.RunID, timeout.SessionID)
		err = handler.AddHandleTask(rc, timeout.ContactID, task)
		if err != nil {
			return errors.Annotatef(err, "error adding new handle task")
		}

		// and mark it as queued
		err = marker.AddTask(rc, markerGroup, taskID)
		if err != nil {
			return errors.Annotatef(err, "error marking timeout task as queued")
		}

		count++
	}

	log.WithField("elapsed", time.Since(start)).WithField("count", count).Info("timeouts queued")
	return nil
}

const timedoutRunsSQL = `
	SELECT 
		r.id as run_id,
		r.flow_id as flow_id,
		r.timeout_on as timeout_on,
		r.session_id as session_id,
		r.contact_id as contact_id,
		r.org_id as org_id
	FROM 
		flows_flowrun r
		JOIN orgs_org o ON r.org_id = o.id
	WHERE 
		is_active = TRUE AND 
		timeout_on > NOW() AND
		o.flow_server_enabled = TRUE
	ORDER BY 
		timedout_on ASC
	LIMIT 25000
`

type Timeout struct {
	RunID     models.FlowRunID `db:"run_id"`
	TimeoutOn time.Time        `db:"timeout_on"`
	FlowID    models.FlowID    `db:"flow_id"`
	SessionID models.SessionID `db:"session_id"`
	ContactID flows.ContactID  `db:"contact_id"`
	OrgID     models.OrgID     `db:"org_id"`
}
