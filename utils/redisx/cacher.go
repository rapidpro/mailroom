package redisx

import (
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/nyaruka/gocommon/dates"
)

var cacherGetScript = redis.NewScript(3,
	`-- KEYS: [TodayKey, YesterdayKey, Key]
local value = redis.call("HGET", KEYS[1], KEYS[3])
if (value ~= false) then
	return value
end
return redis.call("HGET", KEYS[2], KEYS[3])
`)

// Cacher operates like a hash map but values are automatically expired after end of each day
type Cacher struct {
	keyBase string
}

// NewCacher creates a new empty cacher
func NewCacher(keyBase string) *Cacher {
	return &Cacher{keyBase: keyBase}
}

// Get returns the value of the given key
func (c *Cacher) Get(rc redis.Conn, key string) (string, error) {
	todayKey, yesterdayKey := c.keys()

	value, err := redis.String(cacherGetScript.Do(rc, todayKey, yesterdayKey, key))
	if err != nil && err != redis.ErrNil {
		return "", err
	}
	return value, nil
}

// Sets sets the value of the given key
func (c *Cacher) Set(rc redis.Conn, key, value string) error {
	todayKey, _ := c.keys()

	rc.Send("MULTI")
	rc.Send("HSET", todayKey, key, value)
	rc.Send("EXPIRE", todayKey, 60*60*24) // 24 hours
	_, err := rc.Do("EXEC")
	return err
}

// Remove removes the given key
func (c *Cacher) Remove(rc redis.Conn, key string) error {
	todayKey, yesterdayKey := c.keys()

	rc.Send("MULTI")
	rc.Send("HDEL", todayKey, key)
	rc.Send("HDEL", yesterdayKey, key)
	_, err := rc.Do("EXEC")
	return err
}

// ClearAll removes all values
func (c *Cacher) ClearAll(rc redis.Conn) error {
	todayKey, yesterdayKey := c.keys()

	rc.Send("MULTI")
	rc.Send("DEL", todayKey)
	rc.Send("DEL", yesterdayKey)
	_, err := rc.Do("EXEC")
	return err
}

// keys returns the keys for the today set and the yesterday set
func (c *Cacher) keys() (string, string) {
	now := dates.Now()
	today := now.UTC().Format("2006_01_02")
	yesterday := now.Add(time.Hour * -24).UTC().Format("2006_01_02")

	return fmt.Sprintf("%s:%s", c.keyBase, today), fmt.Sprintf("%s:%s", c.keyBase, yesterday)
}
