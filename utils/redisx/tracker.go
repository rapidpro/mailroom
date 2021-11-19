package redisx

import (
	"fmt"
	"strconv"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/nyaruka/gocommon/dates"
)

// StatesTracker is a tracker for counts of different states
type StatesTracker struct {
	keyBase  string        // e.g. channel:23
	states   []string      // e.g. {"success", "failure"}
	interval time.Duration // e.g. 5 minutes
	window   time.Duration // e.g. 30 minutes
}

// NewStatesTracker creates a new states tracker
func NewStatesTracker(keyBase string, states []string, interval time.Duration, window time.Duration) *StatesTracker {
	return &StatesTracker{keyBase: keyBase, states: states, interval: interval, window: window}
}

// Record records a result (i.e. one of the states)
func (t *StatesTracker) Record(rc redis.Conn, s string) error {
	its := t.getIntervalStart(dates.Now().Unix(), 0)
	key := t.getCountKey(its, s)

	// give count key an expiry which ensures it will be around for at least our window
	exp := time.Unix(its, 0).Add(-t.window).Add(-time.Second * 10)

	rc.Send("MULTI")
	rc.Send("INCR", key)
	rc.Send("EXPIREAT", key, exp)
	_, err := rc.Do("EXEC")
	return err
}

// Current returns the total counts of all states, across all intervals within our window
func (t *StatesTracker) Current(rc redis.Conn) (map[string]int, error) {
	now := dates.Now().Unix()                   // now as timestamp
	wStart := now - int64(t.window/time.Second) // start of window as timestamp

	// build a list of count keys of all intervals that fall within our window
	keys := make([]interface{}, 0, 20)

	iEnd := t.getIntervalStart(now, 1) // end of current interval is start of next
	lastINum := 0
	lastIScale := 1.0

	for i := 0; ; i-- {
		iStart := t.getIntervalStart(now, i)
		if iEnd < wStart {
			break
		}

		for _, s := range t.states {
			keys = append(keys, t.getCountKey(iStart, s))
		}

		// if this is the last interval and it starts before the window, calculate how much of it
		// is in the window and we'll scale it's counts by that factor.
		if iStart < wStart {
			lastIScale = float64(iEnd-wStart) / t.interval.Seconds()
		}

		lastINum = i
		iEnd = iStart
	}

	counts, err := redis.Ints(rc.Do("MGET", keys...))
	if err != nil {
		return nil, err
	}

	totals := make(map[string]int, len(t.states))

	for i, j := 0, 0; j < len(counts); i, j = i-1, j+len(t.states) {
		scale := 1.0
		if i == lastINum {
			scale = lastIScale
		}

		for k, s := range t.states {
			totals[s] += int(float64(counts[j+k]) * scale)
		}
	}

	return totals, nil
}

func (t *StatesTracker) getCountKey(ts int64, s string) string {
	return fmt.Sprintf("%s:%d:%s", t.keyBase, ts, s)
}

// gets the timestamp for the start of an interval where 0 is the current, -1 the previous etc
func (t *StatesTracker) getIntervalStart(now int64, delta int) int64 {
	intervalSecs := int64(t.interval / time.Second)
	return ((now / intervalSecs) + int64(delta)) * intervalSecs
}

var boolStates = []string{"true", "false"}

// BoolTracker is convenience for tracking two boolean states
type BoolTracker struct {
	StatesTracker
}

// NewBoolTracker creates a new bool tracker
func NewBoolTracker(keyBase string, interval time.Duration, window time.Duration) *BoolTracker {
	return &BoolTracker{
		StatesTracker: StatesTracker{
			keyBase:  keyBase,
			states:   boolStates,
			interval: interval,
			window:   window,
		},
	}
}

// Record records a bool result
func (t *BoolTracker) Record(rc redis.Conn, b bool) error {
	return t.StatesTracker.Record(rc, strconv.FormatBool(b))
}

// Current returns the total counts of true and false states, across all intervals within our window
func (t *BoolTracker) Current(rc redis.Conn) (int, int, error) {
	totals, err := t.StatesTracker.Current(rc)
	if err != nil {
		return 0, 0, err
	}

	return totals["true"], totals["false"], nil
}
