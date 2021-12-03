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

func TestMarker(t *testing.T) {
	rp := assertredis.TestDB()
	rc := rp.Get()
	defer rc.Close()

	defer assertredis.FlushDB()

	defer dates.SetNowSource(dates.DefaultNowSource)
	setNow := func(d time.Time) { dates.SetNowSource(dates.NewFixedNowSource(d)) }

	setNow(time.Date(2021, 11, 18, 12, 0, 3, 234567, time.UTC))

	marker := redisx.NewMarker("foos")
	marker.Add(rc, "A")
	marker.Add(rc, "B")
	marker.Add(rc, "C")

	assertredis.SMembers(t, rp, "foos_2021_11_18", []string{"A", "B", "C"})
	assertredis.SMembers(t, rp, "foos_2021_11_17", []string{})

	assertContains := func(v string) {
		contains, err := marker.Contains(rc, v)
		assert.NoError(t, err)
		assert.True(t, contains, "expected marker to contain %s", v)
	}
	assertNotContains := func(v string) {
		contains, err := marker.Contains(rc, v)
		assert.NoError(t, err)
		assert.False(t, contains, "expected marker to not contain %s", v)
	}

	assertContains("A")
	assertContains("B")
	assertContains("C")
	assertNotContains("D")

	// move forward a day..
	setNow(time.Date(2021, 11, 19, 12, 0, 3, 234567, time.UTC))

	marker.Add(rc, "D")
	marker.Add(rc, "E")

	assertredis.SMembers(t, rp, "foos_2021_11_19", []string{"D", "E"})
	assertredis.SMembers(t, rp, "foos_2021_11_18", []string{"A", "B", "C"})
	assertredis.SMembers(t, rp, "foos_2021_11_17", []string{})

	assertContains("A")
	assertContains("B")
	assertContains("C")
	assertContains("D")
	assertContains("E")
	assertNotContains("F")

	// move forward again..
	setNow(time.Date(2021, 11, 20, 12, 0, 3, 234567, time.UTC))

	marker.Add(rc, "F")
	marker.Add(rc, "G")

	assertredis.SMembers(t, rp, "foos_2021_11_20", []string{"F", "G"})
	assertredis.SMembers(t, rp, "foos_2021_11_19", []string{"D", "E"})
	assertredis.SMembers(t, rp, "foos_2021_11_18", []string{"A", "B", "C"})
	assertredis.SMembers(t, rp, "foos_2021_11_17", []string{})

	assertNotContains("A") // too old
	assertNotContains("B") // too old
	assertNotContains("C") // too old
	assertContains("D")
	assertContains("E")
	assertContains("F")
	assertContains("G")

	err := marker.Remove(rc, "F") // from today
	require.NoError(t, err)
	err = marker.Remove(rc, "E") // from yesterday
	require.NoError(t, err)

	assertredis.SMembers(t, rp, "foos_2021_11_20", []string{"G"})
	assertredis.SMembers(t, rp, "foos_2021_11_19", []string{"D"})

	assertContains("D")
	assertNotContains("E")
	assertNotContains("F")
	assertContains("G")

	err = marker.ClearAll(rc)
	require.NoError(t, err)

	assertredis.SMembers(t, rp, "foos_2021_11_20", []string{})
	assertredis.SMembers(t, rp, "foos_2021_11_19", []string{})

	assertNotContains("D")
	assertNotContains("E")
	assertNotContains("F")
	assertNotContains("G")
}
