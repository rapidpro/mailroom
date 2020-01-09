package stats

import (
	"context"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/librato"
	"github.com/nyaruka/mailroom"
	"github.com/nyaruka/mailroom/cron"
	"github.com/nyaruka/mailroom/queue"
	"github.com/sirupsen/logrus"
)

const (
	expirationLock = "stats"
)

func init() {
	mailroom.AddInitFunction(StartStatsCron)
}

// StartStatsCron starts our cron job of posting stats every minute
func StartStatsCron(mr *mailroom.Mailroom) error {
	cron.StartCron(mr.Quit, mr.RP, expirationLock, time.Second*60,
		func(lockName string, lockValue string) error {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
			defer cancel()
			return dumpStats(ctx, mr.DB, mr.RP, lockName, lockValue)
		},
	)
	return nil
}

var (
	waitDuration time.Duration
	waitCount    int64
)

// dumpStats calculates a bunch of stats every minute and both logs them and posts them to librato
func dumpStats(ctx context.Context, db *sqlx.DB, rp *redis.Pool, lockName string, lockValue string) error {
	// We wait 15 seconds since we fire at the top of the minute, the same as expirations.
	// That way any metrics related to the size of our queue are a bit more accurate (all expirations can
	// usually be handled in 15 seconds). Something more complicated would take into account the age of
	// the items in our queues.
	time.Sleep(time.Second * 15)

	// get our DB status
	stats := db.Stats()

	rc := rp.Get()
	defer rc.Close()

	// calculate size of batch queue
	batchSize, err := queue.Size(rc, queue.BatchQueue)
	if err != nil {
		logrus.WithError(err).Error("error calculating batch queue size")
	}

	// and size of handler queue
	handlerSize, err := queue.Size(rc, queue.HandlerQueue)
	if err != nil {
		logrus.WithError(err).Error("error calculating handler queue size")
	}

	logrus.WithFields(logrus.Fields{
		"db_idle":      stats.Idle,
		"db_busy":      stats.InUse,
		"db_waiting":   stats.WaitCount - waitCount,
		"db_wait":      stats.WaitDuration - waitDuration,
		"batch_size":   batchSize,
		"handler_size": handlerSize,
	}).Info("current stats")

	librato.Gauge("mr.handler_queue", float64(handlerSize))
	librato.Gauge("mr.batch_queue", float64(batchSize))
	librato.Gauge("mr.db_busy", float64(stats.InUse))
	librato.Gauge("mr.db_idle", float64(stats.Idle))
	librato.Gauge("mr.db_waiting", float64(stats.WaitCount-waitCount))
	librato.Gauge("mr.db_wait_ms", float64((stats.WaitDuration-waitDuration)/time.Millisecond))

	waitCount = stats.WaitCount
	waitDuration = stats.WaitDuration

	return nil
}
