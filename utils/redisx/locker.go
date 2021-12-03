package redisx

import (
	"math/rand"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/pkg/errors"
)

type Locker struct {
	key        string
	expiration time.Duration
}

// NewLocker creates a new locker using the given key and expiration
func NewLocker(key string, expiration time.Duration) *Locker {
	return &Locker{key: key, expiration: expiration}
}

// Grab tries to grab this lock in an atomic operation. It returns the lock value if successful.
// It will retry every second until the retry period has ended, returning empty string if not
// acquired in that time.
func (l *Locker) Grab(rp *redis.Pool, retry time.Duration) (string, error) {
	value := makeRandom(10)                    // generate our lock value
	expires := int(l.expiration / time.Second) // convert our expiration to seconds

	start := time.Now()
	for {
		rc := rp.Get()
		success, err := rc.Do("SET", l.key, value, "EX", expires, "NX")
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

var releaseScript = redis.NewScript(1, `
local lockKey, lockValue = KEYS[1], ARGV[1]

if redis.call("GET", lockKey) == lockValue then
	return redis.call("DEL", lockKey)
else
	return 0
end
`)

// Release releases this lock if the given lock value is correct (i.e we own this lock). It is not an
// error to release a lock that is no longer present.
func (l *Locker) Release(rp *redis.Pool, value string) error {
	rc := rp.Get()
	defer rc.Close()

	// we use lua here because we only want to release the lock if we own it
	_, err := releaseScript.Do(rc, l.key, value)
	return err
}

var expireScript = redis.NewScript(1, `
local lockKey, lockValue, lockExpire = KEYS[1], ARGV[1], ARGV[2]

if redis.call("GET", lockKey) == lockValue then
	return redis.call("EXPIRE", lockKey, lockExpire)
else
	return 0
end
`)

// Extend extends our lock expiration by the passed in number of seconds provided the lock value is correct
func (l *Locker) Extend(rp *redis.Pool, value string, expiration time.Duration) error {
	rc := rp.Get()
	defer rc.Close()

	seconds := int(expiration / time.Second) // convert our expiration to seconds

	// we use lua here because we only want to set the expiration time if we own it
	_, err := expireScript.Do(rc, l.key, value, seconds)
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
