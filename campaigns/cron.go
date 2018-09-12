package campaigns

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/juju/errors"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom"
	"github.com/nyaruka/mailroom/cron"
	"github.com/nyaruka/mailroom/marker"
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
	cron.StartCron(mr.Quit, mr.RedisPool, campaignsLock, time.Second*60,
		func(lockName string, lockValue string) error {
			return fireCampaignEvents(mr, lockName, lockValue)
		},
	)

	return nil
}

// fireCampaignEvents looks for all expired campaign event fires and queues them to be started
func fireCampaignEvents(mr *mailroom.Mailroom, lockName string, lockValue string) error {
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
		return errors.Annotatef(err, "error loading expired campaign events")
	}
	defer rows.Close()

	rc := mr.RedisPool.Get()
	defer rc.Close()

	// while we have rows
	for rows.Next() {
		event := &eventFireTask{}
		err := rows.StructScan(event)
		if err != nil {
			return errors.Annotatef(err, "error reading event fire row")
		}

		log = log.WithField("task", event)

		// check whether this event has already been queued to fire
		taskID := fmt.Sprintf("%d", event.FireID)
		dupe, err := marker.HasTask(rc, campaignsLock, taskID)
		if err != nil {
			return errors.Annotate(err, "error checking task lock")
		}

		if !dupe {
			err = queue.AddTask(rc, eventQueue, campaignEventFireType, fmt.Sprintf("%d", event.OrgID), event, queue.DefaultPriority)
			if err != nil {
				return errors.Annotate(err, "error queuing task")
			}

			err = marker.AddTask(rc, campaignsLock, taskID)
			if err != nil {
				return errors.Annotate(err, "error marking task as queued")
			}
			log.Debug("added task")
		} else {
			log.Debug("ignoring task, already queued")
		}
	}

	log.WithField("elapsed", time.Since(start)).Info("campaign fires complete")
	return nil
}

type eventFireTask struct {
	FireID    int64           `db:"fire_id"       json:"fire_id"`
	ContactID flows.ContactID `db:"contact_id"    json:"contact_id"`
	EventID   int64           `db:"event_id"      json:"event_id"`
	FlowID    flows.FlowID    `db:"flow_id"       json:"flow_id"`
	OrgID     int             `db:"org_id"        json:"org_id"`
	Scheduled time.Time       `db:"scheduled"     json:"scheduled"`
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
