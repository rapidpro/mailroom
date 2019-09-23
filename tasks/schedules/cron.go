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

		tz, err := s.Timezone()
		if err != nil {
			log.WithError(err).Error("error firing schedule, unknown timezone")
			continue
		}

		// if it is a broadcast
		if s.Broadcast() != nil {
			// clone our broadcast, our schedule broadcast is just a template
			bcast := models.CloneBroadcast(s.Broadcast())

			// add our task to send this broadcast
			err = queue.AddTask(rc, queue.BatchQueue, queue.SendBroadcast, int(bcast.OrgID()), bcast, queue.HighPriority)
			if err != nil {
				log.WithError(err).Error("error firing scheduled broadcast")
				continue
			}
			broadcasts++

		} else if s.FlowStart() != nil {
			start := s.FlowStart()

			// insert our flow start
			err := models.InsertFlowStarts(ctx, db, []*models.FlowStart{start})
			if err != nil {
				log.WithError(err).Error("error inserting new flow start for schedule")
				continue
			}

			// add our flow start task
			err = queue.AddTask(rc, queue.BatchQueue, queue.StartFlow, int(start.OrgID()), start, queue.HighPriority)
			if err != nil {
				log.WithError(err).Error("error firing scheduled trigger")
			}

			triggers++
		} else {
			log.Info("schedule found with no associated active broadcast or trigger, ignoring")
			noops++
		}

		// calculate the next fire and update it
		nextFire, err := s.GetNextFire(tz, now)
		if err != nil {
			log.WithError(err).Error("error calculating next fire for schedule")
		}

		// update our next fire for this schedule
		err = s.UpdateFires(ctx, db, now, nextFire)
		if err != nil {
			log.WithError(err).Error("error updating next fire for schedule")
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
