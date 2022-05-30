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

func init() {
	mailroom.AddInitFunction(StartAnalyticsCron)
}

// StartAnalyticsCron starts our cron job of posting stats every minute
func StartAnalyticsCron(rt *runtime.Runtime, wg *sync.WaitGroup, quit chan bool) error {
	cron.Start(quit, rt, "stats", time.Second*60, true,
		func() error {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
			defer cancel()
			return reportAnalytics(ctx, rt)
		},
	)
	return nil
}

var (
	waitDuration time.Duration
	waitCount    int64
)

// calculates a bunch of stats every minute and both logs them and sends them to librato
func reportAnalytics(ctx context.Context, rt *runtime.Runtime) error {
	// We wait 15 seconds since we fire at the top of the minute, the same as expirations.
	// That way any metrics related to the size of our queue are a bit more accurate (all expirations can
	// usually be handled in 15 seconds). Something more complicated would take into account the age of
	// the items in our queues.
	time.Sleep(time.Second * 15)

	// get our DB status
	dbStats := rt.DB.Stats()

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
		"db_idle":      dbStats.Idle,
		"db_busy":      dbStats.InUse,
		"db_waiting":   dbStats.WaitCount - waitCount,
		"db_wait":      dbStats.WaitDuration - waitDuration,
		"batch_size":   batchSize,
		"handler_size": handlerSize,
	}).Info("current analytics")

	librato.Gauge("mr.handler_queue", float64(handlerSize))
	librato.Gauge("mr.batch_queue", float64(batchSize))
	librato.Gauge("mr.db_busy", float64(dbStats.InUse))
	librato.Gauge("mr.db_idle", float64(dbStats.Idle))
	librato.Gauge("mr.db_waiting", float64(dbStats.WaitCount-waitCount))
	librato.Gauge("mr.db_wait_ms", float64((dbStats.WaitDuration-waitDuration)/time.Millisecond))

	waitCount = dbStats.WaitCount
	waitDuration = dbStats.WaitDuration

	return nil
}
