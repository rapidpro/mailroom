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

	setNow(time.Date(2021, 11, 18, 12, 7, 3, 234567, time.UTC))

	assertGet := func(c *redisx.Cacher, k, expected string) {
		actual, err := c.Get(rc, k)
		assert.NoError(t, err, "unexpected error getting key %s", k)
		assert.Equal(t, expected, actual, "expected cache key %s to contain %s", k, expected)
	}

	// create a 24-hour based cacher1
	cacher1 := redisx.NewCacher("foos", time.Hour*24)
	cacher1.Set(rc, "A", "1")
	cacher1.Set(rc, "B", "2")
	cacher1.Set(rc, "C", "3")

	assertredis.Hash(t, rp, "foos:2021-11-18", map[string]string{"A": "1", "B": "2", "C": "3"})
	assertredis.Hash(t, rp, "foos:2021-11-17", map[string]string{})

	assertGet(cacher1, "A", "1")
	assertGet(cacher1, "B", "2")
	assertGet(cacher1, "C", "3")
	assertGet(cacher1, "D", "")

	// move forward a day..
	setNow(time.Date(2021, 11, 19, 12, 7, 3, 234567, time.UTC))

	cacher1.Set(rc, "A", "5")
	cacher1.Set(rc, "B", "6")

	assertredis.Hash(t, rp, "foos:2021-11-19", map[string]string{"A": "5", "B": "6"})
	assertredis.Hash(t, rp, "foos:2021-11-18", map[string]string{"A": "1", "B": "2", "C": "3"})
	assertredis.Hash(t, rp, "foos:2021-11-17", map[string]string{})

	assertGet(cacher1, "A", "5")
	assertGet(cacher1, "B", "6")
	assertGet(cacher1, "C", "3")
	assertGet(cacher1, "D", "")

	// move forward again..
	setNow(time.Date(2021, 11, 20, 12, 7, 3, 234567, time.UTC))

	cacher1.Set(rc, "A", "7")
	cacher1.Set(rc, "Z", "9")

	assertredis.Hash(t, rp, "foos:2021-11-20", map[string]string{"A": "7", "Z": "9"})
	assertredis.Hash(t, rp, "foos:2021-11-19", map[string]string{"A": "5", "B": "6"})
	assertredis.Hash(t, rp, "foos:2021-11-18", map[string]string{"A": "1", "B": "2", "C": "3"})
	assertredis.Hash(t, rp, "foos:2021-11-17", map[string]string{})

	assertGet(cacher1, "A", "7")
	assertGet(cacher1, "Z", "9")
	assertGet(cacher1, "B", "6")
	assertGet(cacher1, "C", "") // too old
	assertGet(cacher1, "D", "")

	err := cacher1.Remove(rc, "A") // from today and yesterday
	require.NoError(t, err)
	err = cacher1.Remove(rc, "B") // from yesterday
	require.NoError(t, err)

	assertredis.Hash(t, rp, "foos:2021-11-20", map[string]string{"Z": "9"})
	assertredis.Hash(t, rp, "foos:2021-11-19", map[string]string{})
	assertredis.Hash(t, rp, "foos:2021-11-18", map[string]string{"A": "1", "B": "2", "C": "3"})
	assertredis.Hash(t, rp, "foos:2021-11-17", map[string]string{})

	assertGet(cacher1, "A", "")
	assertGet(cacher1, "Z", "9")
	assertGet(cacher1, "B", "")
	assertGet(cacher1, "C", "")
	assertGet(cacher1, "D", "")

	err = cacher1.ClearAll(rc)
	require.NoError(t, err)

	assertredis.Hash(t, rp, "foos:2021-11-20", map[string]string{})
	assertredis.Hash(t, rp, "foos:2021-11-19", map[string]string{})

	assertGet(cacher1, "A", "")
	assertGet(cacher1, "Z", "")
	assertGet(cacher1, "B", "")
	assertGet(cacher1, "C", "")
	assertGet(cacher1, "D", "")

	// create a 5 minute based cacher
	cacher2 := redisx.NewCacher("foos", time.Minute*5)
	cacher2.Set(rc, "A", "1")
	cacher2.Set(rc, "B", "2")

	assertredis.Hash(t, rp, "foos:2021-11-20T12:05", map[string]string{"A": "1", "B": "2"})
	assertredis.Hash(t, rp, "foos:2021-11-20T12:00", map[string]string{})

	assertGet(cacher2, "A", "1")
	assertGet(cacher2, "B", "2")
	assertGet(cacher2, "C", "")
}
