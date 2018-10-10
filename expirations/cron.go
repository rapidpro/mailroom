package expirations

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

	// batch of run expirations we'll need to expire at once
	batch := make([]interface{}, 0, expireBatchSize)

	// select our expired runs
	rows, err := db.QueryxContext(ctx, selectExpiredRunsSQL)
	if err != nil {
		return errors.Annotatef(err, "error querying for expired runs")
	}
	defer rows.Close()

	expireBatch := func(batch []interface{}) error {
		err = models.BulkSQL(ctx, "expiring runs", db, expireRunsSQL, batch)
		if err != nil {
			return errors.Annotatef(err, "error expiring runs")
		}

		err = models.BulkSQL(ctx, "expiring sessions", db, expireSessionsSQL, batch)
		if err != nil {
			return errors.Annotatef(err, "error expiring sessions")
		}
		return nil
	}

	count := 0
	for rows.Next() {
		expiration := &RunExpiration{}
		err := rows.StructScan(expiration)
		if err != nil {
			return errors.Annotatef(err, "error scanning expired run")
		}

		count++

		// no parent id? we can add this to our batch
		if expiration.ParentID == nil {
			batch = append(batch, expiration)

			// batch is full? commit it
			if len(batch) == expireBatchSize {
				err = expireBatch(batch)
				if err != nil {
					return err
				}
				batch = batch[:0]
			}

			continue
		}

		// need to continue this session and flow, create a task for that
		taskID := fmt.Sprintf("%d:%s", expiration.RunID, expiration.ExpiresOn.Format(time.RFC3339))
		queued, err := marker.HasTask(rc, markerGroup, taskID)
		if err != nil {
			return errors.Annotatef(err, "error checking whether expiratoin is queued")
		}

		// already queued? move on
		if queued {
			continue
		}

		// ok, queue this task
		task := handler.NewExpirationEvent(expiration.OrgID, expiration.ContactID, expiration.FlowID, expiration.RunID, expiration.SessionID)
		err = handler.AddHandleTask(rc, expiration.ContactID, task)
		if err != nil {
			return errors.Annotatef(err, "error adding new expiration task")
		}

		// and mark it as queued
		err = marker.AddTask(rc, markerGroup, taskID)
		if err != nil {
			return errors.Annotatef(err, "error marking expiration task as queued")
		}
	}

	// commit any stragglers
	if len(batch) > 0 {
		err = expireBatch(batch)
		if err != nil {
			return err
		}
	}

	log.WithField("elapsed", time.Since(start)).WithField("count", count).Info("expirations complete")
	return nil
}

const selectExpiredRunsSQL = `
	SELECT 
		fr.org_id as org_id,	
		fr.flow_id as flow_id,		
		fr.id as run_id, 
		fr.parent_id as parent_id,
		fr.session_id as session_id,
		fr.expires_on as expires_on
	FROM 
		flows_flowrun fr
		JOIN orgs_org o ON fr.org_id = o.id
	WHERE 
		fr.is_active = TRUE AND 
		fr.expires_on < NOW() AND 
		fr.connection_id IS NULL AND
		o.flow_server_enabled = TRUE
	ORDER BY 
		expires_on ASC
	LIMIT 25000
`

const expireRunsSQL = `
	UPDATE 
		flows_flowrun fr
	SET 
		is_active = FALSE, 
		exited_on = NOW(), 
		exit_type = 'E', 
		modified_on = NOW(), 
		child_context = NULL, 
		parent_context = NULL
	FROM
		VALUES(:run_id)) as r(run_id)
	WHERE 
		fr.id = r.run_id::int
`

const expireSessionsSQL = `
	UPDATE 
		flows_flowsession s
	SET 
		is_active = FALSE, 
		ended_on = NOW(), 
		status = 'X' 
	FROM 
		(VALUES(:session_id)) AS r(session_id)
	WHERE
		s.id = r.session_id::int		
`

type RunExpiration struct {
	OrgID     models.OrgID      `db:"org_id"`
	FlowID    models.FlowID     `db:"flow_id"`
	ContactID flows.ContactID   `db:"contact_id"`
	RunID     models.FlowRunID  `db:"run_id"`
	ParentID  *models.FlowRunID `db:"parent_id"`
	SessionID models.SessionID  `db:"session_id"`
	ExpiresOn time.Time         `db:"expires_on"`
}
