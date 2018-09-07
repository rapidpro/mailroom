package campaigns

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/nyaruka/mailroom"
	"github.com/nyaruka/mailroom/cron"
	"github.com/nyaruka/mailroom/marker"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/queue"
	"github.com/sirupsen/logrus"
)

const (
	eventQueue            = "events"
	campaignsLock         = "campaign_event"
	campaignEventFireType = "campaign_event_fire"
)

func init() {
	mailroom.AddInitFunction(StartCampaignCron)
}

// StartCampaignCron starts our cron job of firing expired campaign events
func StartCampaignCron(mr *mailroom.Mailroom) error {
	cron.StartMinuteCron(mr.Quit, mr.RedisPool, campaignsLock,
		func(rc redis.Conn, lockName string, lockValue string) error {
			return fireCampaignEvents(mr, rc, lockName, lockValue)
		},
	)

	return nil
}

type eventFireTask struct {
	FireID    int64            `db:"fire_id"       json:"fire_id"`
	ContactID models.ContactID `db:"contact_id"    json:"contact_id"`
	EventID   int64            `db:"event_id"      json:"event_id"`
	FlowID    models.FlowID    `db:"flow_id"       json:"flow_id"`
	OrgID     int              `db:"org_id"        json:"org_id"`
	Scheduled time.Time        `db:"scheduled"     json:"scheduled"`
}

const expiredEventsQuery = `
SELECT
	ef.id as fire_id, 
	ef.contact_id as contact_id, 
	ef.event_id as event_id,
	f.id as flow_id,
	f.org_id as org_id,
	ef.scheduled as scheduled 	
FROM
	campaigns_eventfire ef, 
	campaigns_campaignevent ce,
	flows_flow f
WHERE
	ef.fired IS NULL AND ef.scheduled < NOW() AND
	ce.id = ef.event_id AND
	f.id = ce.flow_id AND f.is_system = TRUE AND f.flow_server_enabled = TRUE
ORDER BY
	scheduled ASC
LIMIT
	500
`

// expireRuns expires all the runs that have an expiration in the past
func fireCampaignEvents(mr *mailroom.Mailroom, rc redis.Conn, lockName string, lockValue string) error {
	log := logrus.WithField("comp", "campaign_events").WithField("lock", lockValue)
	start := time.Now()

	// find all events that need to be fired
	ctx, cancel := context.WithTimeout(mr.CTX, time.Minute*5)
	defer cancel()

	rows, err := mr.DB.QueryxContext(ctx, expiredEventsQuery)
	if err == sql.ErrNoRows {
		log.WithField("elapsed", time.Since(start)).Info("no events to fire")
		return nil
	}

	if err != nil {
		log.WithError(err).Error("error looking up events fire")
		return err
	}
	defer rows.Close()

	// while we have rows
	event := eventFireTask{}
	for rows.Next() {
		err := rows.StructScan(&event)
		if err != nil {
			log.WithError(err).Error("error reading event fire row")
			return err
		}

		// check whether this event has already been queued to fire
		taskID := fmt.Sprintf("%d", event.FireID)
		dupe, err := marker.HasTask(rc, campaignsLock, taskID)
		if err != nil {
			log.WithError(err).WithField("taskID", taskID).Error("error checking task lock")
			return err
		}

		if !dupe {
			err = queue.AddTask(rc, eventQueue, campaignEventFireType, fmt.Sprintf("%d", event.OrgID), event, queue.DefaultPriority)
			if err != nil {
				log.WithError(err).WithField("taskID", taskID).Error("error queuing task")
				return err
			}

			err = marker.AddTask(rc, campaignsLock, taskID)
			if err != nil {
				log.WithError(err).WithField("taskID", taskID).Error("error marking task as queued")
				return err
			}

			log.WithField("task", event).Debug("added task")
		} else {
			log.WithField("task", event).Debug("ingoring task, already queued")
		}
	}

	log.WithField("elapsed", time.Since(start)).Info("campaign fires complete")
	return nil
}
