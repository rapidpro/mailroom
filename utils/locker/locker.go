package locker

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/pkg/errors"
)

const sleep = time.Second * 1

// GrabLock grabs the passed in lock from redis in an atomic operation. It returns the lock value
// if successful. It will retry until the retry period, returning empty string if not acquired
// in that time.
func GrabLock(rp *redis.Pool, key string, expiration time.Duration, retry time.Duration) (string, error) {
	// generate our lock value
	value := makeRandom(10)

	// convert our expiration to seconds
	seconds := int(expiration / time.Second)
	if seconds < 1 {
		return "", errors.Errorf("can't grab lock with expiration less than a second")
	}

	start := time.Now()
	for {
		rc := rp.Get()
		success, err := rc.Do("SET", fmt.Sprintf("lock:%s", key), value, "EX", seconds, "NX")
		rc.Close()

		if err != nil {
			return "", errors.Wrapf(err, "error trying to get lock")
		}

		if success == "OK" {
			break
		}

		if time.Since(start) > retry {
			return "", nil
		}

		time.Sleep(time.Second)
	}

	return value, nil
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
func ReleaseLock(rp *redis.Pool, key string, value string) error {
	rc := rp.Get()
	defer rc.Close()

	// we use lua here because we only want to release the lock if we own it
	_, err := releaseScript.Do(rc, fmt.Sprintf("lock:%s", key), value)
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
func ExtendLock(rp *redis.Pool, key string, value string, expiration time.Duration) error {
	rc := rp.Get()
	defer rc.Close()

	// convert our expiration to seconds
	seconds := int(expiration / time.Second)
	if seconds < 1 {
		return errors.Errorf("can't grab lock with expiration less than a second")
	}

	// we use lua here because we only want to set the expiration time if we own it
	_, err := expireScript.Do(rc, fmt.Sprintf("lock:%s", key), value, seconds)
	return err
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

// makeRandom creates a random key of the length passed in
func makeRandom(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}
