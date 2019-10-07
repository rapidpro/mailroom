package schedules

import (
	"context"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/mailroom"
	"github.com/nyaruka/mailroom/cron"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/queue"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	scheduleLock = "fire_schedules"
)

func init() {
	mailroom.AddInitFunction(StartCheckSchedules)
}

// StartCheckSchedules starts our cron job of firing schedules every minute
func StartCheckSchedules(mr *mailroom.Mailroom) error {
	cron.StartCron(mr.Quit, mr.RP, scheduleLock, time.Minute*1,
		func(lockName string, lockValue string) error {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
			defer cancel()
			// we sleep 1 second since we fire right on the minute and want to make sure to fire
			// things that are schedules right at the minute as well (and DB time may be slightly drifted)
			time.Sleep(time.Second * 1)
			return checkSchedules(ctx, mr.DB, mr.RP, lockName, lockValue)
		},
	)
	return nil
}

// checkSchedules looks up any expired schedules and fires them, setting the next fire as needed
func checkSchedules(ctx context.Context, db *sqlx.DB, rp *redis.Pool, lockName string, lockValue string) error {
	log := logrus.WithField("comp", "schedules_cron").WithField("lock", lockValue)
	start := time.Now()

	rc := rp.Get()
	defer rc.Close()

	// get any expired schedules
	unfired, err := models.GetUnfiredSchedules(ctx, db)
	if err != nil {
		return errors.Wrapf(err, "error while getting unfired schedules")
	}

	// for each unfired schedule
	broadcasts := 0
	triggers := 0
	noops := 0

	for _, s := range unfired {
		log := log.WithField("schedule_id", s.ID())
		now := time.Now()

		// grab our timezone
		tz, err := s.Timezone()
		if err != nil {
			log.WithError(err).Error("error firing schedule, unknown timezone")
			continue
		}

		// calculate our next fire
		nextFire, err := s.GetNextFire(tz, now)
		if err != nil {
			log.WithError(err).Error("error calculating next fire for schedule")
			continue
		}

		// open a transaction for committing all the items for this fire
		tx, err := db.BeginTxx(ctx, nil)
		if err != nil {
			log.WithError(err).Error("error starting transaction for schedule fire")
			continue
		}

		var task interface{}
		var taskName string

		// if it is a broadcast
		if s.Broadcast() != nil {
			// clone our broadcast, our schedule broadcast is just a template
			bcast, err := models.InsertChildBroadcast(ctx, tx, s.Broadcast())
			if err != nil {
				log.WithError(err).Error("error inserting new broadcast for schedule")
				tx.Rollback()
				continue
			}

			// add our task to send this broadcast
			task = bcast
			taskName = queue.SendBroadcast
			broadcasts++

		} else if s.FlowStart() != nil {
			start := s.FlowStart()

			// insert our flow start
			err := models.InsertFlowStarts(ctx, tx, []*models.FlowStart{start})
			if err != nil {
				log.WithError(err).Error("error inserting new flow start for schedule")
				tx.Rollback()
				continue
			}

			// add our flow start task
			task = start
			taskName = queue.StartFlow
			triggers++
		} else {
			log.Info("schedule found with no associated active broadcast or trigger, ignoring")
			noops++
		}

		// update our next fire for this schedule
		err = s.UpdateFires(ctx, tx, now, nextFire)
		if err != nil {
			log.WithError(err).Error("error updating next fire for schedule")
			tx.Rollback()
			continue
		}

		// commit our transaction
		err = tx.Commit()
		if err != nil {
			log.WithError(err).Error("error comitting schedule transaction")
			tx.Rollback()
			continue
		}

		// add our task if we have one
		if task != nil {
			err = queue.AddTask(rc, queue.BatchQueue, taskName, int(s.OrgID()), task, queue.HighPriority)
			if err != nil {
				log.WithError(err).Error("error firing task with name: ", taskName)
			}
		}
	}

	log.WithFields(logrus.Fields{
		"broadcasts": broadcasts,
		"triggers":   triggers,
		"noops":      noops,
		"elapsed":    time.Since(start),
	}).Info("fired schedules")

	return nil
}
