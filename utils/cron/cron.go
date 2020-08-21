package cron

import (
	"fmt"
	"time"

	"github.com/apex/log"
	"github.com/gomodule/redigo/redis"
	"github.com/nyaruka/mailroom/utils/locker"
	"github.com/sirupsen/logrus"
)

// Function is the function that will be called on our schedule
type Function func(lockName string, lockValue string) error

// StartCron calls the passed in function every minute, making sure it acquires a
// lock so that only one process is running at once. Note that across processes
// crons may be called more often than duration as there is no inter-process
// coordination of cron fires. (this might be a worthy addition)
func StartCron(quit chan bool, rp *redis.Pool, name string, interval time.Duration, cronFunc Function) {
	lockName := fmt.Sprintf("%s_lock", name)
	wait := time.Duration(0)
	lastFire := time.Now()

	log := logrus.WithField("cron", name).WithField("lockName", lockName)

	go func() {
		defer log.Info("exiting")

		// we run expiration every minute on the minute
		for true {
			select {
			case <-quit:
				// we are exiting, return so our goroutine can exit
				return

			case <-time.After(wait):
				// try to insert our expiring lock to redis
				lastFire = time.Now()

				lock, err := locker.GrabLock(rp, lockName, time.Minute*5, 0)
				if err != nil {
					break
				}
				log := log.WithField("lock", lock)

				if lock == "" {
					log.Debug("lock already present, sleeping")
					break
				}

				// ok, got the lock, run our cron function
				err = fireCron(cronFunc, lockName, lock)
				if err != nil {
					log.WithError(err).Error("error while running cron")
				}

				// release our lock
				err = locker.ReleaseLock(rp, lockName, lock)
				if err != nil {
					log.WithError(err).Error("error releasing lock")
				}
			}

			// calculate our next fire time
			nextFire := nextFire(lastFire, interval)
			wait = nextFire.Sub(time.Now())
			if wait < time.Duration(0) {
				wait = time.Duration(0)
			}
		}
	}()
}

// fireCron is just a wrapper around the cron function we will call for the purposes of
// catching and logging panics
func fireCron(cronFunc Function, lockName string, lockValue string) error {
	log := log.WithField("lockValue", lockValue).WithField("func", cronFunc)
	defer func() {
		// catch any panics and recover
		panicLog := recover()
		if panicLog != nil {
			log.Errorf("panic running cron: %s", panicLog)
		}
	}()

	return cronFunc(lockName, lockValue)
}

// nextFire returns the next time we should fire based on the passed in time and interval
func nextFire(last time.Time, interval time.Duration) time.Time {
	if interval >= time.Second && interval < time.Minute {
		normalizedInterval := interval - ((time.Duration(last.Second()) * time.Second) % interval)
		return last.Add(normalizedInterval)
	} else if interval == time.Minute {
		seconds := time.Duration(60-last.Second()) + 1
		return last.Add(seconds * time.Second)
	} else {
		// no special treatment for other things
		return last.Add(interval)
	}
}
