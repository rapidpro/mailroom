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

func TestCache(t *testing.T) {
	rp := assertredis.TestDB()
	rc := rp.Get()
	defer rc.Close()

	defer assertredis.FlushDB()

	defer dates.SetNowSource(dates.DefaultNowSource)
	setNow := func(d time.Time) { dates.SetNowSource(dates.NewFixedNowSource(d)) }

	setNow(time.Date(2021, 11, 18, 12, 7, 3, 234567, time.UTC))

	assertGet := func(c *redisx.Cache, k, expected string) {
		actual, err := c.Get(rc, k)
		assert.NoError(t, err, "unexpected error getting key %s", k)
		assert.Equal(t, expected, actual, "expected cache key %s to contain %s", k, expected)
	}

	// create a 24-hour x 2 based cache
	cache1 := redisx.NewCache("foos", time.Hour*24, 2)
	assert.NoError(t, cache1.Set(rc, "A", "1"))
	assert.NoError(t, cache1.Set(rc, "B", "2"))
	assert.NoError(t, cache1.Set(rc, "C", "3"))

	assertredis.HGetAll(t, rp, "foos:2021-11-18", map[string]string{"A": "1", "B": "2", "C": "3"})
	assertredis.HGetAll(t, rp, "foos:2021-11-17", map[string]string{})

	assertGet(cache1, "A", "1")
	assertGet(cache1, "B", "2")
	assertGet(cache1, "C", "3")
	assertGet(cache1, "D", "")

	// move forward a day..
	setNow(time.Date(2021, 11, 19, 12, 7, 3, 234567, time.UTC))

	cache1.Set(rc, "A", "5")
	cache1.Set(rc, "B", "6")

	assertredis.HGetAll(t, rp, "foos:2021-11-19", map[string]string{"A": "5", "B": "6"})
	assertredis.HGetAll(t, rp, "foos:2021-11-18", map[string]string{"A": "1", "B": "2", "C": "3"})
	assertredis.HGetAll(t, rp, "foos:2021-11-17", map[string]string{})

	assertGet(cache1, "A", "5")
	assertGet(cache1, "B", "6")
	assertGet(cache1, "C", "3")
	assertGet(cache1, "D", "")

	// move forward again..
	setNow(time.Date(2021, 11, 20, 12, 7, 3, 234567, time.UTC))

	cache1.Set(rc, "A", "7")
	cache1.Set(rc, "Z", "9")

	assertredis.HGetAll(t, rp, "foos:2021-11-20", map[string]string{"A": "7", "Z": "9"})
	assertredis.HGetAll(t, rp, "foos:2021-11-19", map[string]string{"A": "5", "B": "6"})
	assertredis.HGetAll(t, rp, "foos:2021-11-18", map[string]string{"A": "1", "B": "2", "C": "3"})
	assertredis.HGetAll(t, rp, "foos:2021-11-17", map[string]string{})

	assertGet(cache1, "A", "7")
	assertGet(cache1, "Z", "9")
	assertGet(cache1, "B", "6")
	assertGet(cache1, "C", "") // too old
	assertGet(cache1, "D", "")

	err := cache1.Remove(rc, "A") // from today and yesterday
	require.NoError(t, err)
	err = cache1.Remove(rc, "B") // from yesterday
	require.NoError(t, err)

	assertredis.HGetAll(t, rp, "foos:2021-11-20", map[string]string{"Z": "9"})
	assertredis.HGetAll(t, rp, "foos:2021-11-19", map[string]string{})
	assertredis.HGetAll(t, rp, "foos:2021-11-18", map[string]string{"A": "1", "B": "2", "C": "3"})
	assertredis.HGetAll(t, rp, "foos:2021-11-17", map[string]string{})

	assertGet(cache1, "A", "")
	assertGet(cache1, "Z", "9")
	assertGet(cache1, "B", "")
	assertGet(cache1, "C", "")
	assertGet(cache1, "D", "")

	err = cache1.ClearAll(rc)
	require.NoError(t, err)

	assertredis.HGetAll(t, rp, "foos:2021-11-20", map[string]string{})
	assertredis.HGetAll(t, rp, "foos:2021-11-19", map[string]string{})

	assertGet(cache1, "A", "")
	assertGet(cache1, "Z", "")
	assertGet(cache1, "B", "")
	assertGet(cache1, "C", "")
	assertGet(cache1, "D", "")

	// create a 5 minute x 3 based cache
	cache2 := redisx.NewCache("foos", time.Minute*5, 3)
	cache2.Set(rc, "A", "1")
	cache2.Set(rc, "B", "2")

	assertredis.HGetAll(t, rp, "foos:2021-11-20T12:05", map[string]string{"A": "1", "B": "2"})
	assertredis.HGetAll(t, rp, "foos:2021-11-20T12:00", map[string]string{})

	assertGet(cache2, "A", "1")
	assertGet(cache2, "B", "2")
	assertGet(cache2, "C", "")
}
