package campaigns

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/librato"
	"github.com/nyaruka/mailroom"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/queue"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/utils/cron"
	"github.com/nyaruka/redisx"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	maxBatchSize = 100
)

var campaignsMarker = redisx.NewIntervalSet("campaign_event", time.Hour*24, 2)

func init() {
	mailroom.AddInitFunction(StartCampaignCron)
}

// StartCampaignCron starts our cron job of firing expired campaign events
func StartCampaignCron(rt *runtime.Runtime, wg *sync.WaitGroup, quit chan bool) error {
	cron.Start(quit, rt, "campaign_event", time.Second*60, false,
		func() error {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
			defer cancel()
			return QueueEventFires(ctx, rt)
		},
	)

	return nil
}

// QueueEventFires looks for all due campaign event fires and queues them to be started
func QueueEventFires(ctx context.Context, rt *runtime.Runtime) error {
	log := logrus.WithField("comp", "campaign_events")
	start := time.Now()

	// find all events that need to be fired
	ctx, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	rows, err := rt.DB.QueryxContext(ctx, expiredEventsQuery)
	if err != nil {
		return errors.Wrapf(err, "error loading expired campaign events")
	}
	defer rows.Close()

	rc := rt.RP.Get()
	defer rc.Close()

	orgID := models.NilOrgID
	var task *FireCampaignEventTask
	numFires, numDupes, numTasks := 0, 0, 0

	for rows.Next() {
		row := &eventFireRow{}
		err := rows.StructScan(row)
		if err != nil {
			return errors.Wrapf(err, "error reading event fire row")
		}

		numFires++

		// check whether this event has already been queued to fire
		taskID := fmt.Sprintf("%d", row.FireID)
		dupe, err := campaignsMarker.Contains(rc, taskID)
		if err != nil {
			return errors.Wrap(err, "error checking task lock")
		}

		// this has already been queued, skip
		if dupe {
			numDupes++
			continue
		}

		// if this is the same event as our current task, and we haven't reached the fire per task limit, add it there
		if task != nil && row.EventID == task.EventID && len(task.FireIDs) < maxBatchSize {
			task.FireIDs = append(task.FireIDs, row.FireID)
			continue
		}

		// if not, queue up current task...
		if task != nil {
			err = queueFiresTask(rt.RP, orgID, task)
			if err != nil {
				return errors.Wrapf(err, "error queueing task")
			}
			numTasks++
		}

		// and create a new one based on this row
		orgID = row.OrgID
		task = &FireCampaignEventTask{
			FireIDs:      []models.FireID{row.FireID},
			EventID:      row.EventID,
			EventUUID:    row.EventUUID,
			FlowUUID:     row.FlowUUID,
			CampaignUUID: row.CampaignUUID,
			CampaignName: row.CampaignName,
		}
	}

	// queue our last task if we have one
	if task != nil {
		if err := queueFiresTask(rt.RP, orgID, task); err != nil {
			return errors.Wrapf(err, "error queueing task")
		}
		numTasks++
	}

	librato.Gauge("mr.campaign_event_cron_elapsed", float64(time.Since(start))/float64(time.Second))
	librato.Gauge("mr.campaign_event_cron_count", float64(numFires))
	log.WithFields(logrus.Fields{
		"elapsed": time.Since(start),
		"fires":   numFires,
		"dupes":   numDupes,
		"tasks":   numTasks,
	}).Info("campaign event fire queuing complete")
	return nil
}

func queueFiresTask(rp *redis.Pool, orgID models.OrgID, task *FireCampaignEventTask) error {
	rc := rp.Get()
	defer rc.Close()

	err := queue.AddTask(rc, queue.BatchQueue, TypeFireCampaignEvent, int(orgID), task, queue.DefaultPriority)
	if err != nil {
		return errors.Wrap(err, "error queuing task")
	}

	// mark each of these fires as queued
	for _, id := range task.FireIDs {
		err = campaignsMarker.Add(rc, fmt.Sprintf("%d", id))
		if err != nil {
			return errors.Wrap(err, "error marking fire as queued")
		}
	}

	logrus.WithField("comp", "campaign_events").WithField("event", task.EventUUID).WithField("fires", len(task.FireIDs)).Debug("queued campaign event fire task")
	return nil
}

type eventFireRow struct {
	FireID       models.FireID   `db:"fire_id"`
	EventID      int64           `db:"event_id"`
	EventUUID    string          `db:"event_uuid"`
	FlowUUID     assets.FlowUUID `db:"flow_uuid"`
	CampaignUUID string          `db:"campaign_uuid"`
	CampaignName string          `db:"campaign_name"`
	OrgID        models.OrgID    `db:"org_id"`
}

const expiredEventsQuery = `
SELECT
    ef.id as fire_id,
    ef.event_id as event_id,
    ce.uuid as event_uuid,
	f.uuid as flow_uuid,
	c.uuid as campaign_uuid,
    c.name as campaign_name,
    f.org_id as org_id
FROM
    campaigns_eventfire ef,
    campaigns_campaignevent ce,
    campaigns_campaign c,
    flows_flow f
WHERE
    ef.fired IS NULL AND ef.scheduled <= NOW() AND
	ce.id = ef.event_id AND
	ce.is_active = TRUE AND
    f.id = ce.flow_id AND
    ce.campaign_id = c.id
ORDER BY
    DATE_TRUNC('minute', scheduled) ASC,
    ef.event_id ASC
LIMIT
    25000;
`
