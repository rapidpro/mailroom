package schedules

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/queue"
	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/nyaruka/mailroom/core/tasks/msgs"
	"github.com/nyaruka/mailroom/core/tasks/starts"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/pkg/errors"
)

func init() {
	tasks.RegisterCron("fire_schedules", false, &schedulesCron{})
}

type schedulesCron struct{}

func (c *schedulesCron) Next(last time.Time) time.Time {
	return tasks.CronNext(last, time.Minute)
}

// checkSchedules looks up any expired schedules and fires them, setting the next fire as needed
func (c *schedulesCron) Run(ctx context.Context, rt *runtime.Runtime) (map[string]any, error) {
	// we sleep 1 second since we fire right on the minute and want to make sure to fire
	// things that are schedules right at the minute as well (and DB time may be slightly drifted)
	time.Sleep(time.Second * 1)

	log := slog.With("comp", "schedules_cron")

	rc := rt.RP.Get()
	defer rc.Close()

	// get any expired schedules
	unfired, err := models.GetUnfiredSchedules(ctx, rt.DB.DB)
	if err != nil {
		return nil, errors.Wrapf(err, "error while getting unfired schedules")
	}

	// for each unfired schedule
	broadcasts := 0
	triggers := 0
	noops := 0

	for _, s := range unfired {
		log := log.With("schedule_id", s.ID())
		now := time.Now()

		// grab our timezone
		tz, err := s.Timezone()
		if err != nil {
			log.Error("error firing schedule, unknown timezone", "error", err)
			continue
		}

		// calculate our next fire
		nextFire, err := s.GetNextFire(tz, now)
		if err != nil {
			log.Error("error calculating next fire for schedule", "error", err)
			continue
		}

		// open a transaction for committing all the items for this fire
		tx, err := rt.DB.BeginTxx(ctx, nil)
		if err != nil {
			log.Error("error starting transaction for schedule fire", "error", err)
			continue
		}

		var task tasks.Task

		// if it is a broadcast
		if s.Broadcast() != nil {
			log = log.With("broadcast_id", s.Broadcast().ID)

			// clone our broadcast, our schedule broadcast is just a template
			bcast, err := models.InsertChildBroadcast(ctx, tx, s.Broadcast())
			if err != nil {
				log.Error("error inserting new broadcast for schedule", "error", err)
				tx.Rollback()
				continue
			}

			// add our task to send this broadcast
			task = &msgs.SendBroadcastTask{Broadcast: bcast}
			broadcasts++

		} else if s.Trigger() != nil {
			log = log.With("trigger_id", s.Trigger().ID())

			start := s.Trigger().CreateStart()

			// insert our flow start
			err := models.InsertFlowStarts(ctx, tx, []*models.FlowStart{start})
			if err != nil {
				log.Error("error inserting new flow start for schedule", "error", err)
				tx.Rollback()
				continue
			}

			// add our flow start task
			task = &starts.StartFlowTask{FlowStart: start}
			triggers++
		} else {
			log.Error("schedule found with no associated active broadcast or trigger")
			noops++
		}

		if nextFire != nil {
			// update our next fire for this schedule
			err = s.UpdateFires(ctx, tx, now, nextFire)
			if err != nil {
				log.Error("error updating next fire for schedule", "error", err)
				tx.Rollback()
				continue
			}
		} else {
			// delete schedule and associated broadcast or trigger
			err = s.DeleteWithTarget(ctx, tx.Tx)
			if err != nil {
				log.Error("error deleting schedule", "error", err)
				tx.Rollback()
				continue
			}
		}

		// commit our transaction
		err = tx.Commit()
		if err != nil {
			log.Error("error comitting schedule transaction", "error", err)
			tx.Rollback()
			continue
		}

		// add our task if we have one
		if task != nil {
			err = tasks.Queue(rc, queue.BatchQueue, s.OrgID(), task, queue.HighPriority)
			if err != nil {
				log.Error(fmt.Sprintf("error queueing %s task from schedule", task.Type()), "error", err)
			}
		}
	}

	return map[string]any{"broadcasts": broadcasts, "triggers": triggers, "noops": noops}, nil
}
