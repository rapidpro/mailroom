package cron

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/nyaruka/gocommon/analytics"
	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/redisx"
)

const (
	statsExpires       = 60 * 60 * 48 // 2 days
	statsKeyBase       = "cron_stats"
	statsLastStartKey  = statsKeyBase + ":last_start"
	statsLastTimeKey   = statsKeyBase + ":last_time"
	statsLastResultKey = statsKeyBase + ":last_result"
	statsCallCountKey  = statsKeyBase + ":call_count"
	statsTotalTimeKey  = statsKeyBase + ":total_time"
)

var statsKeys = []string{
	statsLastStartKey,
	statsLastTimeKey,
	statsLastResultKey,
	statsCallCountKey,
	statsTotalTimeKey,
}

// Function is the function that will be called on our schedule
type Function func(context.Context, *runtime.Runtime) (map[string]any, error)

// Start calls the passed in function every interval, making sure it acquires a
// lock so that only one process is running at once. Note that across processes
// crons may be called more often than duration as there is no inter-process
// coordination of cron fires. (this might be a worthy addition)
func Start(rt *runtime.Runtime, wg *sync.WaitGroup, name string, allInstances bool, cronFunc Function, next func(time.Time) time.Time, timeout time.Duration, quit chan bool) {
	wg.Add(1) // add ourselves to the wait group

	lockName := fmt.Sprintf("lock:%s_lock", name) // for historical reasons...

	// for jobs that run on all instances, the lock key is specific to this instance
	if allInstances {
		lockName = fmt.Sprintf("%s:%s", lockName, rt.Config.InstanceName)
	}

	locker := redisx.NewLocker(lockName, timeout+time.Second*30)

	wait := time.Duration(0)
	lastFire := time.Now()

	log := slog.With("cron", name)

	go func() {
		defer func() {
			log.Info("cron exiting")
			wg.Done()
		}()

		for {
			select {
			case <-quit:
				// we are exiting, return so our goroutine can exit
				return

			case <-time.After(wait):
				lastFire = time.Now()

				// try to get lock but don't retry - if lock is taken then task is still running or running on another instance
				lock, err := locker.Grab(rt.RP, 0)
				if err != nil {
					break
				}

				if lock == "" {
					log.Debug("lock already present, sleeping")
					break
				}

				// ok, got the lock, run our cron function
				started := time.Now()
				results, err := fireCron(rt, name, cronFunc, timeout)
				if err != nil {
					log.Error("error while running cron", "error", err)
				}
				ended := time.Now()

				recordCompletion(rt.RP, name, started, ended, results)

				// release our lock
				err = locker.Release(rt.RP, lock)
				if err != nil {
					log.Error("error releasing lock", "error", err)
				}
			}

			// calculate our next fire time
			nextFire := next(lastFire)
			wait = time.Until(nextFire)
			if wait < time.Duration(0) {
				wait = time.Duration(0)
			}
		}
	}()
}

// fireCron is just a wrapper around the cron function we will call for the purposes of
// catching and logging panics
func fireCron(rt *runtime.Runtime, name string, cronFunc Function, timeout time.Duration) (map[string]any, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	defer func() {
		// catch any panics and recover
		panicLog := recover()
		if panicLog != nil {
			slog.Error(fmt.Sprintf("panic running cron: %s", panicLog), "cron", name)
		}
	}()

	return cronFunc(ctx, rt)
}

func recordCompletion(rp *redis.Pool, name string, started, ended time.Time, results map[string]any) {
	log := slog.With("cron", name)
	elapsed := ended.Sub(started)
	elapsedSeconds := elapsed.Seconds()

	rc := rp.Get()
	defer rc.Close()

	rc.Send("HSET", statsLastStartKey, name, started.Format(time.RFC3339))
	rc.Send("HSET", statsLastTimeKey, name, elapsedSeconds)
	rc.Send("HSET", statsLastResultKey, name, jsonx.MustMarshal(results))
	rc.Send("HINCRBY", statsCallCountKey, name, 1)
	rc.Send("HINCRBYFLOAT", statsTotalTimeKey, name, elapsedSeconds)
	for _, key := range statsKeys {
		rc.Send("EXPIRE", key, statsExpires)
	}

	if err := rc.Flush(); err != nil {
		log.Error("error writing cron results to redis")
	}

	analytics.Gauge("mr.cron_"+name, elapsedSeconds)

	logResults := make([]any, 0, len(results)*2)
	for k, v := range results {
		logResults = append(logResults, k, v)
	}
	log = log.With("elapsed", elapsedSeconds, slog.Group("results", logResults...))

	// if cron too longer than a minute, log as error
	if elapsed > time.Minute {
		log.Error("cron took too long")
	} else {
		log.Info("cron completed")
	}
}
