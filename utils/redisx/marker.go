package redisx

import (
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/nyaruka/gocommon/dates"
)

var markerContainsScript = redis.NewScript(5, `
local currKey, prevKey, value, legacyToday, legacyYesterday = KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5]

local found = redis.call("SISMEMBER", currKey, value)
if found == 1 then
	return 1
end
found = redis.call("SISMEMBER", prevKey, value)
if found == 1 then
	return 1
end
found = redis.call("SISMEMBER", legacyToday, value)
if found == 1 then
	return 1
end
return redis.call("SISMEMBER", legacyYesterday, value)
`)

// Marker operates like a set but values are automatically expired after 24-48 hours
type Marker struct {
	keyBase  string
	interval time.Duration
}

// NewMarker creates a new empty marker
func NewMarker(keyBase string, interval time.Duration) *Marker {
	return &Marker{keyBase: keyBase, interval: interval}
}

// Contains returns whether we contain the given value
func (m *Marker) Contains(rc redis.Conn, value string) (bool, error) {
	currKey, prevKey, legacyToday, legacyYesterday := m.keys()

	return redis.Bool(markerContainsScript.Do(rc, currKey, prevKey, value, legacyToday, legacyYesterday))
}

// Add adds the given value
func (m *Marker) Add(rc redis.Conn, value string) error {
	currKey, _, legacyToday, _ := m.keys()

	rc.Send("MULTI")
	rc.Send("SADD", currKey, value)
	rc.Send("SADD", legacyToday, value)
	rc.Send("EXPIRE", currKey, 60*60*24)     // 24 hours
	rc.Send("EXPIRE", legacyToday, 60*60*24) // 24 hours
	_, err := rc.Do("EXEC")
	return err
}

// Remove removes the given value
func (m *Marker) Remove(rc redis.Conn, value string) error {
	currKey, prevKey, legacyToday, legacyYesterday := m.keys()

	rc.Send("MULTI")
	rc.Send("SREM", currKey, value)
	rc.Send("SREM", prevKey, value)
	rc.Send("SREM", legacyToday, value)
	rc.Send("SREM", legacyYesterday, value)
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
