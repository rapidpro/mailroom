package redisx

import (
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/nyaruka/gocommon/dates"
)

// Cacher operates like a hash map but with expiring values
type Cacher struct {
	keyBase  string
	interval time.Duration
}

// NewCacher creates a new empty cacher
func NewCacher(keyBase string, interval time.Duration) *Cacher {
	return &Cacher{keyBase: keyBase, interval: interval}
}

var cacherGetScript = redis.NewScript(2, `
local currKey, prevKey, field = KEYS[1], KEYS[2], ARGV[1]
local value = redis.call("HGET", currKey, field)
if (value ~= false) then
	return value
end
return redis.call("HGET", prevKey, field)
`)

// Get returns the value of the given field
func (c *Cacher) Get(rc redis.Conn, field string) (string, error) {
	currKey, prevKey := c.keys()

	value, err := redis.String(cacherGetScript.Do(rc, currKey, prevKey, field))
	if err != nil && err != redis.ErrNil {
		return "", err
	}
	return value, nil
}

// Sets sets the value of the given field
func (c *Cacher) Set(rc redis.Conn, field, value string) error {
	currKey, _ := c.keys()

	rc.Send("MULTI")
	rc.Send("HSET", currKey, field, value)
	rc.Send("EXPIRE", currKey, c.interval/time.Second)
	_, err := rc.Do("EXEC")
	return err
}

// Remove removes the given field
func (c *Cacher) Remove(rc redis.Conn, field string) error {
	currKey, prevKey := c.keys()

	rc.Send("MULTI")
	rc.Send("HDEL", currKey, field)
	rc.Send("HDEL", prevKey, field)
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
