package cron

import (
	"fmt"
	"time"

	"github.com/apex/log"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/redisx"
	"github.com/sirupsen/logrus"
)

// Function is the function that will be called on our schedule
type Function func() error

// Start calls the passed in function every interval, making sure it acquires a
// lock so that only one process is running at once. Note that across processes
// crons may be called more often than duration as there is no inter-process
// coordination of cron fires. (this might be a worthy addition)
func Start(quit chan bool, rt *runtime.Runtime, name string, interval time.Duration, allInstances bool, cronFunc Function) {
	lockName := fmt.Sprintf("lock:%s_lock", name) // for historical reasons...

	// for jobs that run on all instances, the lock key is specific to this instance
	if allInstances {
		lockName = fmt.Sprintf("%s:%s", lockName, rt.Config.InstanceName)
	}

	locker := redisx.NewLocker(lockName, time.Minute*5)

	wait := time.Duration(0)
	lastFire := time.Now()

	log := logrus.WithField("cron", name).WithField("lockName", lockName)

	go func() {
		defer log.Info("cron exiting")

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
				err = locker.Release(rt.RP, lock)
				if err != nil {
					log.WithError(err).Error("error releasing lock")
				}
			}

			// calculate our next fire time
			nextFire := NextFire(lastFire, interval)
			wait = time.Until(nextFire)
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

	return cronFunc()
}

// NextFire returns the next time we should fire based on the passed in time and interval
func NextFire(last time.Time, interval time.Duration) time.Time {
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
