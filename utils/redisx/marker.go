package redisx

import (
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/nyaruka/gocommon/dates"
)

// Marker operates like a set but with expiring values
type Marker struct {
	keyBase  string
	interval time.Duration
}

// NewMarker creates a new empty marker
func NewMarker(keyBase string, interval time.Duration) *Marker {
	return &Marker{keyBase: keyBase, interval: interval}
}

var markerContainsScript = redis.NewScript(4, `
local currKey, prevKey, legacyToday, legacyYesterday, member = KEYS[1], KEYS[2], KEYS[3], KEYS[4], ARGV[1]

local found = redis.call("SISMEMBER", currKey, member)
if found == 1 then
	return 1
end
found = redis.call("SISMEMBER", prevKey, member)
if found == 1 then
	return 1
end
found = redis.call("SISMEMBER", legacyToday, member)
if found == 1 then
	return 1
end
return redis.call("SISMEMBER", legacyYesterday, member)
`)

// Contains returns whether we contain the given value
func (m *Marker) Contains(rc redis.Conn, member string) (bool, error) {
	currKey, prevKey, legacyToday, legacyYesterday := m.keys()

	return redis.Bool(markerContainsScript.Do(rc, currKey, prevKey, legacyToday, legacyYesterday, member))
}

// Add adds the given value
func (m *Marker) Add(rc redis.Conn, member string) error {
	currKey, _, legacyToday, _ := m.keys()

	rc.Send("MULTI")
	rc.Send("SADD", currKey, member)
	rc.Send("SADD", legacyToday, member)
	rc.Send("EXPIRE", currKey, m.interval/time.Second)
	rc.Send("EXPIRE", legacyToday, m.interval/time.Second)
	_, err := rc.Do("EXEC")
	return err
}

// Remove removes the given value
func (m *Marker) Remove(rc redis.Conn, member string) error {
	currKey, prevKey, legacyToday, legacyYesterday := m.keys()

	rc.Send("MULTI")
	rc.Send("SREM", currKey, member)
	rc.Send("SREM", prevKey, member)
	rc.Send("SREM", legacyToday, member)
	rc.Send("SREM", legacyYesterday, member)
	_, err := rc.Do("EXEC")
	return err
}

// ClearAll removes all values
func (m *Marker) ClearAll(rc redis.Conn) error {
	currKey, prevKey, legacyToday, legacyYesterday := m.keys()

	rc.Send("MULTI")
	rc.Send("DEL", currKey)
	rc.Send("DEL", prevKey)
	rc.Send("DEL", legacyToday)
	rc.Send("DEL", legacyYesterday)
	_, err := rc.Do("EXEC")
	return err
}

// keys returns the keys for the today set and the yesterday set
func (m *Marker) keys() (string, string, string, string) {
	now := dates.Now()
	currTimestamp := intervalTimestamp(now, m.interval)
	prevTimestamp := intervalTimestamp(now.Add(-m.interval), m.interval)

	legacyToday := fmt.Sprintf("%s_%s", m.keyBase, now.UTC().Format("2006_01_02"))
	legacyYesterday := fmt.Sprintf("%s_%s", m.keyBase, now.Add(time.Hour*-24).UTC().Format("2006_01_02"))

	return fmt.Sprintf("%s:%s", m.keyBase, currTimestamp), fmt.Sprintf("%s:%s", m.keyBase, prevTimestamp), legacyToday, legacyYesterday
}
