package campaigns

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/librato"
	"github.com/nyaruka/mailroom"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/queue"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/utils/cron"
	"github.com/nyaruka/mailroom/utils/marker"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	campaignsLock = "campaign_event"

	maxBatchSize = 100
)

func init() {
	mailroom.AddInitFunction(StartCampaignCron)
}

// StartCampaignCron starts our cron job of firing expired campaign events
func StartCampaignCron(rt *runtime.Runtime, wg *sync.WaitGroup, quit chan bool) error {
	cron.StartCron(quit, rt.RP, campaignsLock, time.Second*60,
		func(lockName string, lockValue string) error {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
			defer cancel()
			return fireCampaignEvents(ctx, rt, lockName, lockValue)
		},
	)

	return nil
}

// fireCampaignEvents looks for all expired campaign event fires and queues them to be started
func fireCampaignEvents(ctx context.Context, rt *runtime.Runtime, lockName string, lockValue string) error {
	log := logrus.WithField("comp", "campaign_events").WithField("lock", lockValue)
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

	queued := 0
	queueTask := func(orgID models.OrgID, task *FireCampaignEventTask) error {
		if task.EventID == 0 {
			return nil
		}

		fireIDs := task.FireIDs
		for len(fireIDs) > 0 {
			batchSize := maxBatchSize
			if batchSize > len(fireIDs) {
				batchSize = len(fireIDs)
			}
			task.FireIDs = fireIDs[:batchSize]
			fireIDs = fireIDs[batchSize:]

			err = queue.AddTask(rc, queue.BatchQueue, TypeFireCampaignEvent, int(orgID), task, queue.DefaultPriority)
			if err != nil {
				return errors.Wrap(err, "error queuing task")
			}

			// mark each of these fires as queued
			for _, id := range task.FireIDs {
				err = marker.AddTask(rc, campaignsLock, fmt.Sprintf("%d", id))
				if err != nil {
					return errors.Wrap(err, "error marking event as queued")
				}
			}
			log.WithField("task", fmt.Sprintf("%vvv", task)).WithField("fire_count", len(task.FireIDs)).Debug("added event fire task")
			queued += len(task.FireIDs)
		}

		return nil
	}

	// while we have rows
	orgID := models.NilOrgID
	task := &FireCampaignEventTask{}

	for rows.Next() {
		row := &eventFireRow{}
		err := rows.StructScan(row)
		if err != nil {
			return errors.Wrapf(err, "error reading event fire row")
		}

		// check whether this event has already been queued to fire
		taskID := fmt.Sprintf("%d", row.FireID)
		dupe, err := marker.HasTask(rc, campaignsLock, taskID)
		if err != nil {
			return errors.Wrap(err, "error checking task lock")
		}

		// this has already been queued, move on
		if dupe {
			continue
		}

		// if this is the same event as our current task, add it there
		if row.EventID == task.EventID {
			task.FireIDs = append(task.FireIDs, row.FireID)
			continue
		}

		// different task, queue up our current task
		err = queueTask(orgID, task)
		if err != nil {
			return errors.Wrapf(err, "error queueing task")
		}

		// and create a new one based on this row
		orgID = row.OrgID
		task = &FireCampaignEventTask{
			FireIDs:      []int64{row.FireID},
			EventID:      row.EventID,
			EventUUID:    row.EventUUID,
			FlowUUID:     row.FlowUUID,
			CampaignUUID: row.CampaignUUID,
			CampaignName: row.CampaignName,
		}
	}

	// queue our last task
	err = queueTask(orgID, task)
	if err != nil {
		return errors.Wrapf(err, "error queueing task")
	}

	librato.Gauge("mr.campaign_event_cron_elapsed", float64(time.Since(start))/float64(time.Second))
	librato.Gauge("mr.campaign_event_cron_count", float64(queued))
	log.WithField("elapsed", time.Since(start)).WithField("queued", queued).Info("campaign event fire queuing complete")
	return nil
}

type eventFireRow struct {
	FireID       int64           `db:"fire_id"`
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
