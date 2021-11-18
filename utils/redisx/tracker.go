package redisx

import (
	"fmt"
	"strconv"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/nyaruka/gocommon/dates"
)

// BoolTracker is a tracker for bool results
type BoolTracker struct {
	keyBase  string        // e.g. channel:23
	interval time.Duration // e.g. 5 minutes
	window   time.Duration // e.g. 30 minutes
}

// NewBool creates a new bool tracker
func NewBoolTracker(keyBase string, interval time.Duration, window time.Duration) *BoolTracker {
	return &BoolTracker{keyBase: keyBase, interval: interval, window: window}
}

// Record records a bool result
func (t *BoolTracker) Record(rc redis.Conn, b bool) error {
	its := t.getIntervalStart(dates.Now().Unix(), 0)
	key := t.getCountKey(its, b)
	exp := time.Unix(its, 0).Add(-t.window)

	rc.Send("MULTI")
	rc.Send("INCR", key)
	rc.Send("EXPIREAT", key, exp)
	_, err := rc.Do("EXEC")
	return err
}

// Current returns the current totals of true and false results
func (t *BoolTracker) Current(rc redis.Conn) (int, int, error) {
	from := dates.Now().Add(-t.window).Unix()

	// build a list of keys of each available interval in pairs of ..:yes, ..:no
	keys := make([]interface{}, 0, 20)

	for i := 0; ; i-- {
		ts := t.getIntervalStart(dates.Now().Unix(), i)
		if ts < from {
			break
		}

		keys = append(keys, t.getCountKey(ts, true), t.getCountKey(ts, false))
	}

	counts, err := redis.Ints(rc.Do("MGET", keys...))
	if err != nil {
		return 0, 0, err
	}

	totalTrue := 0
	totalFalse := 0
	for i, c := range counts {
		if i%2 == 0 {
			totalTrue += c
		} else {
			totalFalse += c
		}
	}

	return totalTrue, totalFalse, nil
}

func (t *BoolTracker) getCountKey(ts int64, b bool) string {
	return fmt.Sprintf("%s:%d:%s", t.keyBase, ts, strconv.FormatBool(b))
}

// gets the timestamp for the start of an interval where 0 is the current, -1 the previous etc
func (t *BoolTracker) getIntervalStart(nowTS int64, delta int) int64 {
	intervalSecs := int64(t.interval / time.Second)
	return ((nowTS / intervalSecs) + int64(delta)) * intervalSecs
}
