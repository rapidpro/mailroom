package redisx

import (
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/nyaruka/gocommon/dates"
)

var markerContainsScript = redis.NewScript(3,
	`-- KEYS: [TodayKey, YesterdayKey, Value]
local found = redis.call("SISMEMBER", KEYS[1], KEYS[3])
if found == 1 then
	return 1
end
return redis.call("SISMEMBER", KEYS[2], KEYS[3])
`)

// Marker operates like a set but values are automatically expired after 24-48 hours
type Marker struct {
	keyBase string
}

// NewMarker creates a new empty marker
func NewMarker(keyBase string) *Marker {
	return &Marker{keyBase: keyBase}
}

// Contains returns whether we contain the given value
func (m *Marker) Contains(rc redis.Conn, value string) (bool, error) {
	todayKey, yesterdayKey := m.keys()

	return redis.Bool(markerContainsScript.Do(rc, todayKey, yesterdayKey, value))
}

// Add adds the given value
func (m *Marker) Add(rc redis.Conn, value string) error {
	todayKey, _ := m.keys()

	rc.Send("MULTI")
	rc.Send("SADD", todayKey, value)
	rc.Send("EXPIRE", todayKey, 60*60*24) // 24 hours
	_, err := rc.Do("EXEC")
	return err
}

// Remove removes the given value
func (m *Marker) Remove(rc redis.Conn, value string) error {
	todayKey, yesterdayKey := m.keys()

	rc.Send("MULTI")
	rc.Send("SREM", todayKey, value)
	rc.Send("SREM", yesterdayKey, value)
	_, err := rc.Do("EXEC")
	return err
}

// ClearAll removes all values
func (m *Marker) ClearAll(rc redis.Conn) error {
	todayKey, yesterdayKey := m.keys()

	rc.Send("MULTI")
	rc.Send("DEL", todayKey)
	rc.Send("DEL", yesterdayKey)
	_, err := rc.Do("EXEC")
	return err
}

// keys returns the keys for the today set and the yesterday set
func (m *Marker) keys() (string, string) {
	now := dates.Now()
	today := now.UTC().Format("2006_01_02")
	yesterday := now.Add(time.Hour * -24).UTC().Format("2006_01_02")

	return fmt.Sprintf("%s_%s", m.keyBase, today), fmt.Sprintf("%s_%s", m.keyBase, yesterday)
}
