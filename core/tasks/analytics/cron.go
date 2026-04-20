package analytics

import (
	"context"
	"log/slog"
	"time"

	"github.com/nyaruka/gocommon/analytics"
	"github.com/nyaruka/mailroom/core/queue"
	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/nyaruka/mailroom/runtime"
)

func init() {
	tasks.RegisterCron("analytics", true, &analyticsCron{})
}

// calculates a bunch of stats every minute and both logs them and sends them to librato
type analyticsCron struct {
	// both sqlx and redis provide wait stats which are cummulative that we need to make into increments
	dbWaitDuration    time.Duration
	dbWaitCount       int64
	redisWaitDuration time.Duration
	redisWaitCount    int64
}

func (c *analyticsCron) Next(last time.Time) time.Time {
	return tasks.CronNext(last, time.Minute)
}

func (c *analyticsCron) Run(ctx context.Context, rt *runtime.Runtime) (map[string]any, error) {
	// We wait 15 seconds since we fire at the top of the minute, the same as expirations.
	// That way any metrics related to the size of our queue are a bit more accurate (all expirations can
	// usually be handled in 15 seconds). Something more complicated would take into account the age of
	// the items in our queues.
	time.Sleep(time.Second * 15)

	rc := rt.RP.Get()
	defer rc.Close()

	// calculate size of batch queue
	batchSize, err := queue.Size(rc, queue.BatchQueue)
	if err != nil {
		slog.Error("error calculating batch queue size", "error", err)
	}

	// and size of handler queue
	handlerSize, err := queue.Size(rc, queue.HandlerQueue)
	if err != nil {
		slog.Error("error calculating handler queue size", "error", err)
	}

	// get our DB and redis stats
	dbStats := rt.DB.Stats()
	redisStats := rt.RP.Stats()

	dbWaitDurationInPeriod := dbStats.WaitDuration - c.dbWaitDuration
	dbWaitCountInPeriod := dbStats.WaitCount - c.dbWaitCount
	redisWaitDurationInPeriod := redisStats.WaitDuration - c.redisWaitDuration
	redisWaitCountInPeriod := redisStats.WaitCount - c.redisWaitCount

	c.dbWaitDuration = dbStats.WaitDuration
	c.dbWaitCount = dbStats.WaitCount
	c.redisWaitDuration = redisStats.WaitDuration
	c.redisWaitCount = redisStats.WaitCount

	analytics.Gauge("mr.db_busy", float64(dbStats.InUse))
	analytics.Gauge("mr.db_idle", float64(dbStats.Idle))
	analytics.Gauge("mr.db_wait_ms", float64(dbWaitDurationInPeriod/time.Millisecond))
	analytics.Gauge("mr.db_wait_count", float64(dbWaitCountInPeriod))
	analytics.Gauge("mr.redis_wait_ms", float64(redisWaitDurationInPeriod/time.Millisecond))
	analytics.Gauge("mr.redis_wait_count", float64(redisWaitCountInPeriod))
	analytics.Gauge("mr.handler_queue", float64(handlerSize))
	analytics.Gauge("mr.batch_queue", float64(batchSize))

	return map[string]any{
		"db_busy":          dbStats.InUse,
		"db_idle":          dbStats.Idle,
		"db_wait_time":     dbWaitDurationInPeriod,
		"db_wait_count":    dbWaitCountInPeriod,
		"redis_wait_time":  dbWaitDurationInPeriod,
		"redis_wait_count": dbWaitCountInPeriod,
		"handler_size":     handlerSize,
		"batch_size":       batchSize,
	}, nil
}
