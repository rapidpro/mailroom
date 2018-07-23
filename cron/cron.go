package cron

import (
	"math/rand"

	"github.com/gomodule/redigo/redis"
)

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

func SetLockExpiraton(rc redis.Conn, key string, value string, expiration int) error {
	// we use lua here because we only want to set the expiration time if we own it
	_, err := expireScript.Do(rc, key, value, expiration)
	return err
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func MakeKey(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}
