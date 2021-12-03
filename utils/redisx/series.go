package redisx

import (
	"time"

	"github.com/gomodule/redigo/redis"
)

// Series returns all values from interval based sets.
type Series struct {
	keyBase  string
	interval time.Duration // e.g. 5 minutes
	size     int           // number of intervals
}

// NewSeries creates a new empty series
func NewSeries(keyBase string, interval time.Duration, size int) *Series {
	return &Series{keyBase: keyBase, interval: interval, size: size}
}

// Record increments the value of field by value in the current interval
func (s *Series) Record(rc redis.Conn, field string, value int64) error {
	currKey := s.keys()[0]

	rc.Send("MULTI")
	rc.Send("HINCRBY", currKey, field, value)
	rc.Send("EXPIRE", currKey, s.size*int(s.interval/time.Second))
	_, err := rc.Do("EXEC")
	return err
}

var seriesGetScript = redis.NewScript(-1, `
local field = ARGV[1]

local values = {}
for _, key in ipairs(KEYS) do
	table.insert(values, redis.call("HGET", key, field))
end

return values
`)

// Get gets the value of field in all intervals
func (s *Series) Get(rc redis.Conn, field string) ([]int64, error) {
	keys := s.keys()
	args := redis.Args{}.Add(len(keys)).AddFlat(keys).Add(field)

	return redis.Int64s(seriesGetScript.Do(rc, args...))
}

func (s *Series) keys() []string {
	return intervalKeys(s.keyBase, s.interval, s.size)
}
