package redisx

import (
	"fmt"
	"time"

	"github.com/nyaruka/gocommon/dates"
)

func intervalTimestamp(t time.Time, interval time.Duration) string {
	t = t.UTC().Truncate(interval)
	if interval < time.Hour*24 {
		return t.Format("2006-01-02T15:04")
	}
	return t.Format("2006-01-02")
}

func intervalKeys(keyBase string, interval time.Duration, size int) []string {
	now := dates.Now()
	keys := make([]string, size)
	for i := range keys {
		timestamp := intervalTimestamp(now.Add(-interval*time.Duration(i)), interval)
		keys[i] = fmt.Sprintf("%s:%s", keyBase, timestamp)
	}
	return keys
}
