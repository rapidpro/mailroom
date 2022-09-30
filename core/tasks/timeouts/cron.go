package timeouts

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
	timeoutLock = "sessions_timeouts"
)

var marker = redisx.NewIntervalSet("session_timeouts", time.Hour*24, 2)

func init() {
	mailroom.AddInitFunction(StartTimeoutCron)
}

// StartTimeoutCron starts our cron job of continuing timed out sessions every minute
func StartTimeoutCron(rt *runtime.Runtime, wg *sync.WaitGroup, quit chan bool) error {
	cron.Start(quit, rt, timeoutLock, time.Second*time.Duration(rt.Config.TimeoutTime), false,
		func() error {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
			defer cancel()
			return timeoutSessions(ctx, rt)
		},
	)
	return nil
}

// timeoutRuns looks for any runs that have timed out and schedules for them to continue
// TODO: extend lock
func timeoutSessions(ctx context.Context, rt *runtime.Runtime) error {
	log := logrus.WithField("comp", "timeout")
	start := time.Now()

	// find all sessions that need to be expired (we exclude IVR runs)
	rows, err := rt.DB.QueryxContext(ctx, timedoutSessionsSQL)
	if err != nil {
		return errors.Wrapf(err, "error selecting timed out sessions")
	}
	defer rows.Close()

	rc := rt.RP.Get()
	defer rc.Close()

	numQueued, numDupes := 0, 0

	// add a timeout task for each run
	timeout := &Timeout{}
	for rows.Next() {
		err := rows.StructScan(timeout)
		if err != nil {
			return errors.Wrapf(err, "error scanning timeout")
		}

		// check whether we've already queued this
		taskID := fmt.Sprintf("%d:%s", timeout.SessionID, timeout.TimeoutOn.Format(time.RFC3339))
		queued, err := marker.Contains(rc, taskID)
		if err != nil {
			return errors.Wrapf(err, "error checking whether task is queued")
		}

		// already queued? move on
		if queued {
			numDupes++
			continue
		}

		// ok, queue this task
		task := handler.NewTimeoutTask(timeout.OrgID, timeout.ContactID, timeout.SessionID, timeout.TimeoutOn)
		err = handler.QueueHandleTask(rc, timeout.ContactID, task)
		if err != nil {
			return errors.Wrapf(err, "error adding new handle task")
		}

		// and mark it as queued
		err = marker.Add(rc, taskID)
		if err != nil {
			return errors.Wrapf(err, "error marking timeout task as queued")
		}

		numQueued++
	}

	log.WithField("dupes", numDupes).WithField("queued", numQueued).WithField("elapsed", time.Since(start)).Info("session timeouts queued")
	return nil
}

const timedoutSessionsSQL = `
	SELECT 
		s.id as session_id,
		s.timeout_on as timeout_on,
		s.contact_id as contact_id,
		s.org_id as org_id
	FROM 
		flows_flowsession s
		JOIN orgs_org o ON s.org_id = o.id
	WHERE 
		status = 'W' AND 
		timeout_on < NOW() AND
		connection_id IS NULL
	ORDER BY 
		timeout_on ASC
	LIMIT 25000
`

type Timeout struct {
	OrgID     models.OrgID     `db:"org_id"`
	FlowID    models.FlowID    `db:"flow_id"`
	ContactID models.ContactID `db:"contact_id"`
	SessionID models.SessionID `db:"session_id"`
	TimeoutOn time.Time        `db:"timeout_on"`
}
