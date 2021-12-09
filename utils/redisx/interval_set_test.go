package redisx_test

import (
	"testing"
	"time"

	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/mailroom/utils/redisx"
	"github.com/nyaruka/mailroom/utils/redisx/assertredis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntervalSet(t *testing.T) {
	rp := assertredis.TestDB()
	rc := rp.Get()
	defer rc.Close()

	defer assertredis.FlushDB()

	defer dates.SetNowSource(dates.DefaultNowSource)
	setNow := func(d time.Time) { dates.SetNowSource(dates.NewFixedNowSource(d)) }

	setNow(time.Date(2021, 11, 18, 12, 0, 3, 234567, time.UTC))

	// create a 24-hour x 2 based set
	marker1 := redisx.NewIntervalSet("foos", time.Hour*24, 2)
	assert.NoError(t, marker1.Add(rc, "A"))
	assert.NoError(t, marker1.Add(rc, "B"))
	assert.NoError(t, marker1.Add(rc, "C"))

	assertredis.SMembers(t, rp, "foos:2021-11-18", []string{"A", "B", "C"})
	assertredis.SMembers(t, rp, "foos:2021-11-17", []string{})

	assertContains := func(s *redisx.IntervalSet, v string) {
		contains, err := s.Contains(rc, v)
		assert.NoError(t, err)
		assert.True(t, contains, "expected marker to contain %s", v)
	}
	assertNotContains := func(s *redisx.IntervalSet, v string) {
		contains, err := s.Contains(rc, v)
		assert.NoError(t, err)
		assert.False(t, contains, "expected marker to not contain %s", v)
	}

	assertContains(marker1, "A")
	assertContains(marker1, "B")
	assertContains(marker1, "C")
	assertNotContains(marker1, "D")

	// move forward a day..
	setNow(time.Date(2021, 11, 19, 12, 0, 3, 234567, time.UTC))

	marker1.Add(rc, "D")
	marker1.Add(rc, "E")

	assertredis.SMembers(t, rp, "foos:2021-11-19", []string{"D", "E"})
	assertredis.SMembers(t, rp, "foos:2021-11-18", []string{"A", "B", "C"})
	assertredis.SMembers(t, rp, "foos:2021-11-17", []string{})

	assertContains(marker1, "A")
	assertContains(marker1, "B")
	assertContains(marker1, "C")
	assertContains(marker1, "D")
	assertContains(marker1, "E")
	assertNotContains(marker1, "F")

	// move forward again..
	setNow(time.Date(2021, 11, 20, 12, 7, 3, 234567, time.UTC))

	marker1.Add(rc, "F")
	marker1.Add(rc, "G")

	assertredis.SMembers(t, rp, "foos:2021-11-20", []string{"F", "G"})
	assertredis.SMembers(t, rp, "foos:2021-11-19", []string{"D", "E"})
	assertredis.SMembers(t, rp, "foos:2021-11-18", []string{"A", "B", "C"})
	assertredis.SMembers(t, rp, "foos:2021-11-17", []string{})

	assertNotContains(marker1, "A") // too old
	assertNotContains(marker1, "B") // too old
	assertNotContains(marker1, "C") // too old
	assertContains(marker1, "D")
	assertContains(marker1, "E")
	assertContains(marker1, "F")
	assertContains(marker1, "G")

	err := marker1.Remove(rc, "F") // from today
	require.NoError(t, err)
	err = marker1.Remove(rc, "E") // from yesterday
	require.NoError(t, err)

	assertredis.SMembers(t, rp, "foos:2021-11-20", []string{"G"})
	assertredis.SMembers(t, rp, "foos:2021-11-19", []string{"D"})

	assertContains(marker1, "D")
	assertNotContains(marker1, "E")
	assertNotContains(marker1, "F")
	assertContains(marker1, "G")

	err = marker1.ClearAll(rc)
	require.NoError(t, err)

	assertredis.SMembers(t, rp, "foos:2021-11-20", []string{})
	assertredis.SMembers(t, rp, "foos:2021-11-19", []string{})

	assertNotContains(marker1, "D")
	assertNotContains(marker1, "E")
	assertNotContains(marker1, "F")
	assertNotContains(marker1, "G")

	// create a 5 minute x 3 based marker
	marker2 := redisx.NewIntervalSet("foos", time.Minute*5, 3)
	marker2.Add(rc, "A")
	marker2.Add(rc, "B")

	assertredis.SMembers(t, rp, "foos:2021-11-20T12:05", []string{"A", "B"})
	assertredis.SMembers(t, rp, "foos:2021-11-20T12:00", []string{})

	assertContains(marker2, "A")
	assertContains(marker2, "B")
	assertNotContains(marker2, "C")
}
