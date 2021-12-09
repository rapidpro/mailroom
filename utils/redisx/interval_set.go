package redisx

import (
	"time"

	"github.com/gomodule/redigo/redis"
)

// IntervalSet operates like a set but with expiring intervals
type IntervalSet struct {
	keyBase  string
	interval time.Duration // e.g. 5 minutes
	size     int           // number of intervals
}

// NewIntervalSet creates a new empty interval set
func NewIntervalSet(keyBase string, interval time.Duration, size int) *IntervalSet {
	return &IntervalSet{keyBase: keyBase, interval: interval, size: size}
}

var isetContainsScript = redis.NewScript(-1, `
local member = ARGV[1]

for _, key in ipairs(KEYS) do
	local found = redis.call("SISMEMBER", key, member)
	if found == 1 then
		return 1
	end
end

return 0
`)

// Contains returns whether we contain the given value
func (s *IntervalSet) Contains(rc redis.Conn, member string) (bool, error) {
	keys := s.keys()

	return redis.Bool(isetContainsScript.Do(rc, redis.Args{}.Add(len(keys)).AddFlat(keys).Add(member)...))
}

// Add adds the given value
func (s *IntervalSet) Add(rc redis.Conn, member string) error {
	key := s.keys()[0]

	rc.Send("MULTI")
	rc.Send("SADD", key, member)
	rc.Send("EXPIRE", key, s.size*int(s.interval/time.Second))
	_, err := rc.Do("EXEC")
	return err
}

// Remove removes the given value
func (s *IntervalSet) Remove(rc redis.Conn, member string) error {
	rc.Send("MULTI")
	for _, k := range s.keys() {
		rc.Send("SREM", k, member)
	}
	_, err := rc.Do("EXEC")
	return err
}

// ClearAll removes all values
func (s *IntervalSet) ClearAll(rc redis.Conn) error {
	rc.Send("MULTI")
	for _, k := range s.keys() {
		rc.Send("DEL", k)
	}
	_, err := rc.Do("EXEC")
	return err
}

func (s *IntervalSet) keys() []string {
	return intervalKeys(s.keyBase, s.interval, s.size)
}
