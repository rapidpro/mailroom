package redisx

import (
	"time"

	"github.com/gomodule/redigo/redis"
)

// IntervalHash operates like a hash map but with expiring intervals
type IntervalHash struct {
	keyBase  string
	interval time.Duration // e.g. 5 minutes
	size     int           // number of intervals
}

// NewIntervalHash creates a new empty interval hash
func NewIntervalHash(keyBase string, interval time.Duration, size int) *IntervalHash {
	return &IntervalHash{keyBase: keyBase, interval: interval, size: size}
}

var ihashGetScript = redis.NewScript(-1, `
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
func (h *IntervalHash) Get(rc redis.Conn, field string) (string, error) {
	keys := h.keys()

	value, err := redis.String(ihashGetScript.Do(rc, redis.Args{}.Add(len(keys)).AddFlat(keys).Add(field)...))
	if err != nil && err != redis.ErrNil {
		return "", err
	}
	return value, nil
}

// Sets sets the value of the given field
func (h *IntervalHash) Set(rc redis.Conn, field, value string) error {
	key := h.keys()[0]

	rc.Send("MULTI")
	rc.Send("HSET", key, field, value)
	rc.Send("EXPIRE", key, h.size*int(h.interval/time.Second))
	_, err := rc.Do("EXEC")
	return err
}

// Remove removes the given field
func (h *IntervalHash) Remove(rc redis.Conn, field string) error {
	rc.Send("MULTI")
	for _, k := range h.keys() {
		rc.Send("HDEL", k, field)
	}
	_, err := rc.Do("EXEC")
	return err
}

// ClearAll removes all values
func (h *IntervalHash) ClearAll(rc redis.Conn) error {
	rc.Send("MULTI")
	for _, k := range h.keys() {
		rc.Send("DEL", k)
	}
	_, err := rc.Do("EXEC")
	return err
}

func (h *IntervalHash) keys() []string {
	return intervalKeys(h.keyBase, h.interval, h.size)
}
