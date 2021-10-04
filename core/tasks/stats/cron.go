package stats

import (
	"context"
	"sync"
	"time"

	"github.com/nyaruka/librato"
	"github.com/nyaruka/mailroom"
	"github.com/nyaruka/mailroom/core/queue"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/utils/cron"
	"github.com/sirupsen/logrus"
)

const (
	expirationLock = "stats"
)

func init() {
	mailroom.AddInitFunction(StartStatsCron)
}

// StartStatsCron starts our cron job of posting stats every minute
func StartStatsCron(rt *runtime.Runtime, wg *sync.WaitGroup, quit chan bool) error {
	cron.StartCron(quit, rt.RP, expirationLock, time.Second*60,
		func(lockName string, lockValue string) error {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
			defer cancel()
			return dumpStats(ctx, rt, lockName, lockValue)
		},
	)
	return nil
}

var (
	waitDuration time.Duration
	waitCount    int64
)

// dumpStats calculates a bunch of stats every minute and both logs them and posts them to librato
func dumpStats(ctx context.Context, rt *runtime.Runtime, lockName string, lockValue string) error {
	// We wait 15 seconds since we fire at the top of the minute, the same as expirations.
	// That way any metrics related to the size of our queue are a bit more accurate (all expirations can
	// usually be handled in 15 seconds). Something more complicated would take into account the age of
	// the items in our queues.
	time.Sleep(time.Second * 15)

	// get our DB status
	stats := rt.DB.Stats()

	rc := rt.RP.Get()
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
