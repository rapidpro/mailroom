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

func TestCacher(t *testing.T) {
	rp := assertredis.TestDB()
	rc := rp.Get()
	defer rc.Close()

	defer assertredis.FlushDB()

	defer dates.SetNowSource(dates.DefaultNowSource)
	setNow := func(d time.Time) { dates.SetNowSource(dates.NewFixedNowSource(d)) }

	setNow(time.Date(2021, 11, 18, 12, 0, 3, 234567, time.UTC))

	cacher := redisx.NewCacher("foos")
	cacher.Set(rc, "A", "1")
	cacher.Set(rc, "B", "2")
	cacher.Set(rc, "C", "3")

	assertredis.Hash(t, rp, "foos:2021_11_18", map[string]string{"A": "1", "B": "2", "C": "3"})
	assertredis.Hash(t, rp, "foos:2021_11_17", map[string]string{})

	assertGet := func(k, expected string) {
		actual, err := cacher.Get(rc, k)
		assert.NoError(t, err, "unexpected error getting key %s", k)
		assert.Equal(t, expected, actual, "expected cache key %s to contain %s", k, expected)
	}

	assertGet("A", "1")
	assertGet("B", "2")
	assertGet("C", "3")
	assertGet("D", "")

	// move forward a day..
	setNow(time.Date(2021, 11, 19, 12, 0, 3, 234567, time.UTC))

	cacher.Set(rc, "A", "5")
	cacher.Set(rc, "B", "6")

	assertredis.Hash(t, rp, "foos:2021_11_19", map[string]string{"A": "5", "B": "6"})
	assertredis.Hash(t, rp, "foos:2021_11_18", map[string]string{"A": "1", "B": "2", "C": "3"})
	assertredis.Hash(t, rp, "foos:2021_11_17", map[string]string{})

	assertGet("A", "5")
	assertGet("B", "6")
	assertGet("C", "3")
	assertGet("D", "")

	// move forward again..
	setNow(time.Date(2021, 11, 20, 12, 0, 3, 234567, time.UTC))

	cacher.Set(rc, "A", "7")
	cacher.Set(rc, "Z", "9")

	assertredis.Hash(t, rp, "foos:2021_11_20", map[string]string{"A": "7", "Z": "9"})
	assertredis.Hash(t, rp, "foos:2021_11_19", map[string]string{"A": "5", "B": "6"})
	assertredis.Hash(t, rp, "foos:2021_11_18", map[string]string{"A": "1", "B": "2", "C": "3"})
	assertredis.Hash(t, rp, "foos:2021_11_17", map[string]string{})

	assertGet("A", "7")
	assertGet("Z", "9")
	assertGet("B", "6")
	assertGet("C", "") // too old
	assertGet("D", "")

	err := cacher.Remove(rc, "A") // from today and yesterday
	require.NoError(t, err)
	err = cacher.Remove(rc, "B") // from yesterday
	require.NoError(t, err)

	assertredis.Hash(t, rp, "foos:2021_11_20", map[string]string{"Z": "9"})
	assertredis.Hash(t, rp, "foos:2021_11_19", map[string]string{})
	assertredis.Hash(t, rp, "foos:2021_11_18", map[string]string{"A": "1", "B": "2", "C": "3"})
	assertredis.Hash(t, rp, "foos:2021_11_17", map[string]string{})

	assertGet("A", "")
	assertGet("Z", "9")
	assertGet("B", "")
	assertGet("C", "")
	assertGet("D", "")

	err = cacher.ClearAll(rc)
	require.NoError(t, err)

	assertredis.Hash(t, rp, "foos:2021_11_20", map[string]string{})
	assertredis.Hash(t, rp, "foos:2021_11_19", map[string]string{})

	assertGet("A", "")
	assertGet("Z", "")
	assertGet("B", "")
	assertGet("C", "")
	assertGet("D", "")
}
