package cron

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/sirupsen/logrus"
)

// Function is the function that will be called on our schedule
type Function func(rc redis.Conn, lockName string, lockValue string) error

// StartMinuteCron calls the passed in function every minute, making sure it acquires a
// lock so that only one process is running at once
func StartMinuteCron(quit chan bool, rp *redis.Pool, name string, cronFunc Function) {
	lockName := fmt.Sprintf("%s_lock", name)
	wait := 0
	go func() {
		// we run expiration every minute on the minute
		for true {

			select {
			case <-quit:
				// we are exiting, return so our goroutine can exit
				return

			case <-time.After(time.Second * time.Duration(wait)):
				rc := rp.Get()

				// try to insert our expiring lock to redis
				lockValue := makeKey(10)
				log := logrus.WithField("comp", name).WithField("lockName", lockName).WithField("lockValue", lockValue)

				locked, err := GrabLock(rc, lockName, lockValue, 300)
				if err != nil {
					log.WithError(err).Error("error acquiring lock")
					err := ReleaseLock(rc, lockName, lockValue)
					if err != nil {
						log.WithError(err).Error("error releasing lock")
					}
					break
				}

				if !locked {
					log.Info("lock already present, sleeping")
					break
				}

				// ok, got the lock, go expire our runs
				err = cronFunc(rc, lockName, lockValue)
				if err != nil {
					log.WithError(err).Error("error while running cron")
				}

				// release our lock
				err = ReleaseLock(rc, lockName, lockValue)
				if err != nil {
					log.WithError(err).Error("error releasing lock")
				}

				rc.Close()
			}

			wait = 61 - time.Now().Second()
		}
	}()
}

// GrabLock grabs the passed in lock from redis in an atomic operation. It returns
// whether the lock was available and acquired
func GrabLock(rc redis.Conn, key string, value string, expiration int) (bool, error) {
	success, err := rc.Do("SET", key, value, "EX", expiration, "NX")
	if err != nil {
		return false, err
	}

	return success != "", nil
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

// ExtendLockExpiration extends our lock expiration by the passed in number of seconds
func ExtendLockExpiration(rc redis.Conn, key string, value string, expiration int) error {
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
