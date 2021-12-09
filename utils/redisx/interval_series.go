package redisx

import (
	"time"

	"github.com/gomodule/redigo/redis"
)

// IntervalSeries returns all values from interval based hashes.
type IntervalSeries struct {
	keyBase  string
	interval time.Duration // e.g. 5 minutes
	size     int           // number of intervals
}

// NewIntervalSeries creates a new empty series
func NewIntervalSeries(keyBase string, interval time.Duration, size int) *IntervalSeries {
	return &IntervalSeries{keyBase: keyBase, interval: interval, size: size}
}

// Record increments the value of field by value in the current interval
func (s *IntervalSeries) Record(rc redis.Conn, field string, value int64) error {
	currKey := s.keys()[0]

	rc.Send("MULTI")
	rc.Send("HINCRBY", currKey, field, value)
	rc.Send("EXPIRE", currKey, s.size*int(s.interval/time.Second))
	_, err := rc.Do("EXEC")
	return err
}

var iseriesGetScript = redis.NewScript(-1, `
local field = ARGV[1]

local values = {}
for _, key in ipairs(KEYS) do
	table.insert(values, redis.call("HGET", key, field))
end

return values
`)

// Get gets the values of field in all intervals
func (s *IntervalSeries) Get(rc redis.Conn, field string) ([]int64, error) {
	keys := s.keys()
	args := redis.Args{}.Add(len(keys)).AddFlat(keys).Add(field)

	return redis.Int64s(iseriesGetScript.Do(rc, args...))
}

// Total gets the total value of field across all intervals
func (s *IntervalSeries) Total(rc redis.Conn, field string) (int64, error) {
	vals, err := s.Get(rc, field)
	if err != nil {
		return 0, err
	}
	var total int64
	for _, v := range vals {
		total += v
	}
	return total, nil
}

func (s *IntervalSeries) keys() []string {
	return intervalKeys(s.keyBase, s.interval, s.size)
}
