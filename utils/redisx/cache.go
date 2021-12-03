package redisx

import (
	"time"

	"github.com/gomodule/redigo/redis"
)

// Cache operates like a hash map but with expiring values
type Cache struct {
	keyBase  string
	interval time.Duration // e.g. 5 minutes
	size     int           // number of intervals
}

// NewCache creates a new empty cache
func NewCache(keyBase string, interval time.Duration, size int) *Cache {
	return &Cache{keyBase: keyBase, interval: interval, size: size}
}

var cacheGetScript = redis.NewScript(-1, `
local field = ARGV[1]

for _, key in ipairs(KEYS) do
	local value = redis.call("HGET", key, field)
	if (value ~= false) then
		return value
	end
end

return false
`)

// Get returns the value of the given field
func (c *Cache) Get(rc redis.Conn, field string) (string, error) {
	keys := c.keys()

	value, err := redis.String(cacheGetScript.Do(rc, redis.Args{}.Add(len(keys)).AddFlat(keys).Add(field)...))
	if err != nil && err != redis.ErrNil {
		return "", err
	}
	return value, nil
}

// Sets sets the value of the given field
func (c *Cache) Set(rc redis.Conn, field, value string) error {
	key := c.keys()[0]

	rc.Send("MULTI")
	rc.Send("HSET", key, field, value)
	rc.Send("EXPIRE", key, c.size*int(c.interval/time.Second))
	_, err := rc.Do("EXEC")
	return err
}

// Remove removes the given field
func (c *Cache) Remove(rc redis.Conn, field string) error {
	rc.Send("MULTI")
	for _, k := range c.keys() {
		rc.Send("HDEL", k, field)
	}
	_, err := rc.Do("EXEC")
	return err
}

// ClearAll removes all values
func (c *Cache) ClearAll(rc redis.Conn) error {
	rc.Send("MULTI")
	for _, k := range c.keys() {
		rc.Send("DEL", k)
	}
	_, err := rc.Do("EXEC")
	return err
}

func (c *Cache) keys() []string {
	return intervalKeys(c.keyBase, c.interval, c.size)
}
