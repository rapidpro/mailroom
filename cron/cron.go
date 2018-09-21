package cron

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/apex/log"
	"github.com/gomodule/redigo/redis"
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
		// we run expiration every minute on the minute
		for true {
			select {
			case <-quit:
				// we are exiting, return so our goroutine can exit
				return

			case <-time.After(wait):
				// try to insert our expiring lock to redis
				lastFire = time.Now()
				lockValue := makeKey(10)
				log := log.WithField("lockValue", lockValue)

				rc := rp.Get()
				locked, err := GrabLock(rc, lockName, lockValue, 300)
				rc.Close()
				if err != nil {
					break
				}

				if !locked {
					log.Debug("lock already present, sleeping")
					break
				}

				// ok, got the lock, run our cron function
				err = fireCron(cronFunc, lockName, lockValue)
				if err != nil {
					log.WithError(err).Error("error while running cron")
				}

				// release our lock
				rc = rp.Get()
				err = ReleaseLock(rc, lockName, lockValue)
				rc.Close()
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

// GrabLock grabs the passed in lock from redis in an atomic operation. It returns
// whether the lock was available and acquired
func GrabLock(rc redis.Conn, key string, value string, expiration int) (bool, error) {
	success, err := rc.Do("SET", key, value, "EX", expiration, "NX")
	if err != nil {
		return false, err
	}

	return success == "OK", nil
}

var releaseScript = redis.NewScript(2, `
    -- KEYS: [Key, Value]
	if redis.call("get", KEYS[1]) == KEYS[2] then
      return redis.call("del", KEYS[1])
    else
      return 0
    end
`)

// ReleaseLock releases the passed in lock, returning any error encountered while doing
// so. It is not considered an error to release a lock that is no longer present
func ReleaseLock(rc redis.Conn, key string, value string) error {
	// we use lua here because we only want to release the lock if we own it
	_, err := releaseScript.Do(rc, key, value)
	return err
}

var expireScript = redis.NewScript(3, `
    -- KEYS: [Key, Value, Expiration]
	  if redis.call("get", KEYS[1]) == KEYS[2] then
      return redis.call("expire", KEYS[1], KEYS[3])
    else
      return 0
    end
`)

// ExtendLock extends our lock expiration by the passed in number of seconds
func ExtendLock(rc redis.Conn, key string, value string, expiration int) error {
	// we use lua here because we only want to set the expiration time if we own it
	_, err := expireScript.Do(rc, key, value, expiration)
	return err
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

// makeKey creates a random key of the length passed in
func makeKey(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
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
