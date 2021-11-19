package redisx

import (
	"fmt"
	"strconv"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/nyaruka/gocommon/dates"
)

// StatesTracker is a tracker for bool results
type StatesTracker struct {
	keyBase  string // e.g. channel:23
	states   []string
	interval time.Duration // e.g. 5 minutes
	window   time.Duration // e.g. 30 minutes
}

// NewStatesTracker creates a new states tracker
func NewStatesTracker(keyBase string, states []string, interval time.Duration, window time.Duration) *StatesTracker {
	return &StatesTracker{keyBase: keyBase, states: states, interval: interval, window: window}
}

// Record records a bool result
func (t *StatesTracker) Record(rc redis.Conn, s string) error {
	its := t.getIntervalStart(dates.Now(), 0)
	key := t.getCountKey(its, s)
	exp := time.Unix(its, 0).Add(-t.window)

	rc.Send("MULTI")
	rc.Send("INCR", key)
	rc.Send("EXPIREAT", key, exp)
	_, err := rc.Do("EXEC")
	return err
}

// Current returns the current totals of all states
func (t *StatesTracker) Current(rc redis.Conn) (map[string]int, error) {
	now := dates.Now()
	from := now.Add(-t.window).Unix()

	// build a list of keys of each available interval in pairs of ..:yes, ..:no
	keys := make([]interface{}, 0, 20)

	for i := 0; ; i-- {
		ts := t.getIntervalStart(now, i)
		if ts < from {
			break
		}

		for _, s := range t.states {
			keys = append(keys, t.getCountKey(ts, s))
		}
	}

	counts, err := redis.Ints(rc.Do("MGET", keys...))
	if err != nil {
		return nil, err
	}

	totals := make(map[string]int, len(t.states))

	for i := 0; i < len(counts); i += len(t.states) {
		for j, s := range t.states {
			totals[s] += counts[i+j]
		}
	}

	return totals, nil
}

func (t *StatesTracker) getCountKey(ts int64, s string) string {
	return fmt.Sprintf("%s:%d:%s", t.keyBase, ts, s)
}

// gets the timestamp for the start of an interval where 0 is the current, -1 the previous etc
func (t *StatesTracker) getIntervalStart(now time.Time, delta int) int64 {
	intervalSecs := int64(t.interval / time.Second)
	return ((now.Unix() / intervalSecs) + int64(delta)) * intervalSecs
}

// BoolTracker is a tracker for bool results
type BoolTracker struct {
	StatesTracker
}

// NewBoolTracker creates a new bool tracker
func NewBoolTracker(keyBase string, interval time.Duration, window time.Duration) *BoolTracker {
	return &BoolTracker{
		StatesTracker: StatesTracker{
			keyBase:  keyBase,
			states:   []string{"true", "false"},
			interval: interval,
			window:   window,
		},
	}
}

// Record records a bool result
func (t *BoolTracker) Record(rc redis.Conn, b bool) error {
	return t.StatesTracker.Record(rc, strconv.FormatBool(b))
}

// Current returns the current totals of true and false results
func (t *BoolTracker) Current(rc redis.Conn) (int, int, error) {
	totals, err := t.StatesTracker.Current(rc)
	if err != nil {
		return 0, 0, err
	}

	return totals["true"], totals["false"], nil
}
