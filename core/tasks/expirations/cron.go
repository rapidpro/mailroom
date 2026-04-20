package expirations

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/nyaruka/mailroom/core/ivr"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/nyaruka/mailroom/core/tasks/handler"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/redisx"
	"github.com/pkg/errors"
)

const (
	expireBatchSize = 500
)

func init() {
	tasks.RegisterCron("run_expirations", false, NewExpirationsCron())
	tasks.RegisterCron("expire_ivr_calls", false, &VoiceExpirationsCron{})
}

type ExpirationsCron struct {
	marker *redisx.IntervalSet
}

func NewExpirationsCron() *ExpirationsCron {
	return &ExpirationsCron{
		marker: redisx.NewIntervalSet("run_expirations", time.Hour*24, 2),
	}
}

func (c *ExpirationsCron) Next(last time.Time) time.Time {
	return tasks.CronNext(last, time.Minute)
}

// handles waiting messaging sessions whose waits have expired, resuming those that can be resumed,
// and expiring those that can't
func (c *ExpirationsCron) Run(ctx context.Context, rt *runtime.Runtime) (map[string]any, error) {
	rc := rt.RP.Get()
	defer rc.Close()

	// we expire sessions that can't be resumed in batches
	expiredSessions := make([]models.SessionID, 0, expireBatchSize)

	// select messaging sessions with expired waits
	rows, err := rt.DB.QueryxContext(ctx, sqlSelectExpiredWaits)
	if err != nil {
		return nil, errors.Wrapf(err, "error querying for expired waits")
	}
	defer rows.Close()

	numExpired, numDupes, numQueued := 0, 0, 0

	for rows.Next() {
		expiredWait := &ExpiredWait{}
		err := rows.StructScan(expiredWait)
		if err != nil {
			return nil, errors.Wrapf(err, "error scanning expired wait")
		}

		// if it can't be resumed, add to batch to be expired
		if !expiredWait.WaitResumes {
			expiredSessions = append(expiredSessions, expiredWait.SessionID)

			// batch is full? commit it
			if len(expiredSessions) == expireBatchSize {
				err = models.ExitSessions(ctx, rt.DB, expiredSessions, models.SessionStatusExpired)
				if err != nil {
					return nil, errors.Wrapf(err, "error expiring batch of sessions")
				}
				expiredSessions = expiredSessions[:0]
			}

			numExpired++
			continue
		}

		// create a contact task to resume this session
		taskID := fmt.Sprintf("%d:%s", expiredWait.SessionID, expiredWait.WaitExpiresOn.Format(time.RFC3339))
		queued, err := c.marker.IsMember(rc, taskID)
		if err != nil {
			return nil, errors.Wrapf(err, "error checking whether expiration is queued")
		}

		// already queued? move on
		if queued {
			numDupes++
			continue
		}

		// ok, queue this task
		task := handler.NewExpirationTask(expiredWait.OrgID, expiredWait.ContactID, expiredWait.SessionID, expiredWait.WaitExpiresOn)
		err = handler.QueueHandleTask(rc, expiredWait.ContactID, task)
		if err != nil {
			return nil, errors.Wrapf(err, "error adding new expiration task")
		}

		// and mark it as queued
		err = c.marker.Add(rc, taskID)
		if err != nil {
			return nil, errors.Wrapf(err, "error marking expiration task as queued")
		}

		numQueued++
	}

	// commit any stragglers
	if len(expiredSessions) > 0 {
		err = models.ExitSessions(ctx, rt.DB, expiredSessions, models.SessionStatusExpired)
		if err != nil {
			return nil, errors.Wrapf(err, "error expiring runs and sessions")
		}
	}

	return map[string]any{"expired": numExpired, "dupes": numDupes, "queued": numQueued}, nil
}

const sqlSelectExpiredWaits = `
    SELECT s.id as session_id, s.org_id, s.wait_expires_on, s.wait_resume_on_expire , s.contact_id
      FROM flows_flowsession s
     WHERE s.session_type = 'M' AND s.status = 'W' AND s.wait_expires_on <= NOW()
  ORDER BY s.wait_expires_on ASC
     LIMIT 25000`

type ExpiredWait struct {
	SessionID     models.SessionID `db:"session_id"`
	OrgID         models.OrgID     `db:"org_id"`
	WaitExpiresOn time.Time        `db:"wait_expires_on"`
	WaitResumes   bool             `db:"wait_resume_on_expire"`
	ContactID     models.ContactID `db:"contact_id"`
}

type VoiceExpirationsCron struct{}

func (c *VoiceExpirationsCron) Next(last time.Time) time.Time {
	return tasks.CronNext(last, time.Minute)
}

// looks for voice sessions that should be expired and ends them
func (c *VoiceExpirationsCron) Run(ctx context.Context, rt *runtime.Runtime) (map[string]any, error) {
	log := slog.With("comp", "ivr_cron_expirer")

	ctx, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	// select voice sessions with expired waits
	rows, err := rt.DB.QueryxContext(ctx, sqlSelectExpiredVoiceWaits)
	if err != nil {
		return nil, errors.Wrapf(err, "error querying for expired waits")
	}
	defer rows.Close()

	expiredSessions := make([]models.SessionID, 0, 100)
	clogs := make([]*models.ChannelLog, 0, 100)

	for rows.Next() {
		expiredWait := &ExpiredVoiceWait{}
		err := rows.StructScan(expiredWait)
		if err != nil {
			return nil, errors.Wrapf(err, "error scanning expired wait")
		}

		// add the session to those we need to expire
		expiredSessions = append(expiredSessions, expiredWait.SessionID)

		// load our call
		conn, err := models.GetCallByID(ctx, rt.DB, expiredWait.OrgID, expiredWait.CallID)
		if err != nil {
			log.Error("unable to load call", "error", err, "call_id", expiredWait.CallID)
			continue
		}

		// hang up our call
		clog, err := ivr.HangupCall(ctx, rt, conn)
		if err != nil {
			// log error but carry on with other calls
			log.Error("error hanging up call", "error", err, "call_id", conn.ID())
		}

		if clog != nil {
			clogs = append(clogs, clog)
		}
	}

	// now expire our runs and sessions
	if len(expiredSessions) > 0 {
		err := models.ExitSessions(ctx, rt.DB, expiredSessions, models.SessionStatusExpired)
		if err != nil {
			log.Error("error expiring sessions for expired calls", "error", err)
		}
	}

	if err := models.InsertChannelLogs(ctx, rt, clogs); err != nil {
		return nil, errors.Wrap(err, "error inserting channel logs")
	}

	return map[string]any{"expired": len(expiredSessions)}, nil
}

const sqlSelectExpiredVoiceWaits = `
  SELECT id, org_id, call_id, wait_expires_on
    FROM flows_flowsession
   WHERE session_type = 'V' AND status = 'W' AND wait_expires_on <= NOW()
ORDER BY wait_expires_on ASC
   LIMIT 100`

type ExpiredVoiceWait struct {
	SessionID models.SessionID `db:"id"`
	OrgID     models.OrgID     `db:"org_id"`
	CallID    models.CallID    `db:"call_id"`
	ExpiresOn time.Time        `db:"wait_expires_on"`
}
