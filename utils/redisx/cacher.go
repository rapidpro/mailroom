package redisx

import (
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/nyaruka/gocommon/dates"
)

var cacherGetScript = redis.NewScript(3, `
local currKey, prevKey, key = KEYS[1], KEYS[2], KEYS[3]
local value = redis.call("HGET", currKey, key)
if (value ~= false) then
	return value
end
return redis.call("HGET", prevKey, key)
`)

// Cacher operates like a hash map but values are automatically expired
type Cacher struct {
	keyBase  string
	interval time.Duration
}

// NewCacher creates a new empty cacher
func NewCacher(keyBase string, interval time.Duration) *Cacher {
	return &Cacher{keyBase: keyBase, interval: interval}
}

// Get returns the value of the given key
func (c *Cacher) Get(rc redis.Conn, key string) (string, error) {
	currKey, prevKey := c.keys()

	value, err := redis.String(cacherGetScript.Do(rc, currKey, prevKey, key))
	if err != nil && err != redis.ErrNil {
		return "", err
	}
	return value, nil
}

// Sets sets the value of the given key
func (c *Cacher) Set(rc redis.Conn, key, value string) error {
	currKey, _ := c.keys()

	rc.Send("MULTI")
	rc.Send("HSET", currKey, key, value)
	rc.Send("EXPIRE", currKey, 60*60*24) // 24 hours
	_, err := rc.Do("EXEC")
	return err
}

// Remove removes the given key
func (c *Cacher) Remove(rc redis.Conn, key string) error {
	currKey, prevKey := c.keys()

	rc.Send("MULTI")
	rc.Send("HDEL", currKey, key)
	rc.Send("HDEL", prevKey, key)
	_, err := rc.Do("EXEC")
	return err
}

// ClearAll removes all values
func (c *Cacher) ClearAll(rc redis.Conn) error {
	currKey, prevKey := c.keys()

	rc.Send("MULTI")
	rc.Send("DEL", currKey)
	rc.Send("DEL", prevKey)
	_, err := rc.Do("EXEC")
	return err
}

// keys returns the keys for the current set and the previous set
func (c *Cacher) keys() (string, string) {
	now := dates.Now()
	currTimestamp := intervalTimestamp(now, c.interval)
	prevTimestamp := intervalTimestamp(now.Add(-c.interval), c.interval)

	return fmt.Sprintf("%s:%s", c.keyBase, currTimestamp), fmt.Sprintf("%s:%s", c.keyBase, prevTimestamp)
}
