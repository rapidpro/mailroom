package redisx

import "time"

func intervalTimestamp(t time.Time, interval time.Duration) string {
	t = t.UTC().Truncate(interval)
	if interval < time.Hour*24 {
		return t.Format("2006-01-02T15:04")
	}
	return t.Format("2006-01-02")
}
