package tasks_test

import (
	"testing"
	"time"

	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/stretchr/testify/assert"
)

func TestNextFire(t *testing.T) {
	tcs := []struct {
		last     time.Time
		interval time.Duration
		expected time.Time
	}{
		{time.Date(2000, 1, 1, 1, 1, 4, 0, time.UTC), time.Minute, time.Date(2000, 1, 1, 1, 2, 1, 0, time.UTC)},
		{time.Date(2000, 1, 1, 1, 1, 44, 0, time.UTC), time.Minute, time.Date(2000, 1, 1, 1, 2, 1, 0, time.UTC)},
		{time.Date(2000, 1, 1, 1, 1, 1, 100, time.UTC), time.Millisecond * 150, time.Date(2000, 1, 1, 1, 1, 1, 150000100, time.UTC)},
		{time.Date(2000, 1, 1, 2, 6, 1, 0, time.UTC), time.Minute * 10, time.Date(2000, 1, 1, 2, 16, 1, 0, time.UTC)},
		{time.Date(2000, 1, 1, 1, 1, 4, 0, time.UTC), time.Second * 15, time.Date(2000, 1, 1, 1, 1, 15, 0, time.UTC)},
	}

	for _, tc := range tcs {
		actual := tasks.CronNext(tc.last, tc.interval)
		assert.Equal(t, tc.expected, actual, "next fire mismatch for %s + %s", tc.last, tc.interval)
	}
}
