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

func TestStatesTracker(t *testing.T) {
	rp := assertredis.TestDB()
	rc := rp.Get()
	defer rc.Close()

	defer assertredis.FlushDB()

	defer dates.SetNowSource(dates.DefaultNowSource)
	setNow := func(d time.Time) { dates.SetNowSource(dates.NewFixedNowSource(d)) }

	// create a states tracker with interval of 10 seconds and window of 30
	tr := redisx.NewStatesTracker("foo:1", []string{"yes", "no", "maybe"}, time.Second*10, time.Second*30)

	// set now to 12:00:03.. so current interval is 12:00:00 (1637236800) <= t < 12:00:10 (1637236810)
	setNow(time.Date(2021, 11, 18, 12, 0, 3, 234567, time.UTC))

	recordState := func(s string, n int) { require.NoError(t, tr.Record(rc, s, n)) }

	// no counts.. zeros for all
	totals, err := tr.Current(rc)
	assert.NoError(t, err)
	assert.Equal(t, map[string]int{"yes": 0, "no": 0, "maybe": 0}, totals)

	recordState("yes", 8)
	recordState("no", 4)

	assertredis.Int(t, rp, "foo:1:1637236800:yes", 8)
	assertredis.Int(t, rp, "foo:1:1637236800:no", 4)
	assertredis.NotExists(t, rp, "foo:1:1637236800:maybe")

	// current window is 11:59:33 (1637236773) - 12:00:03 (1637236803) so we'll include counts from intervals..
	//  0: 12:00:00 (1637236800) <= t < 12:00:10 (1637236810) yes=8 no=4 maybe=0
	// -1: 11:59:50 (1637236790) <= t < 12:00:00 (1637236800) yes=0 no=0 maybe=0
	// -2: 11:59:40 (1637236780) <= t < 11:59:50 (1637236790) yes=0 no=0 maybe=0
	// -3: 11:59:30 (1637236770) <= t < 11:59:40 (1637236780) yes=0 no=0 maybe=0
	totals, err = tr.Current(rc)
	assert.NoError(t, err)
	assert.Equal(t, map[string]int{"yes": 8, "no": 4, "maybe": 0}, totals)

	// set now to exactly 12:00:10 which falls on the start of a new interval
	setNow(time.Date(2021, 11, 18, 12, 0, 10, 0, time.UTC))

	recordState("yes", 3)
	recordState("no", 1)
	recordState("maybe", 1)

	assertredis.Int(t, rp, "foo:1:1637236800:yes", 8)
	assertredis.Int(t, rp, "foo:1:1637236800:no", 4)
	assertredis.Int(t, rp, "foo:1:1637236810:yes", 3)
	assertredis.Int(t, rp, "foo:1:1637236810:no", 1)
	assertredis.Int(t, rp, "foo:1:1637236810:maybe", 1)

	// current window is 11:40:00 (1637236780) - 12:00:10 (1637236810) so we'll include counts from intervals..
	//  0: 12:00:10 (1637236810) <= t < 12:00:20 (1637236820) yes=3 no=1 maybe=1
	// -1: 12:00:00 (1637236800) <= t < 12:00:10 (1637236810) yes=8 no=4 maybe=0
	// -2: 11:59:50 (1637236790) <= t < 12:00:00 (1637236800) yes=0 no=0 maybe=0
	// -3: 11:59:40 (1637236780) <= t < 11:59:50 (1637236790) yes=0 no=0 maybe=0
	totals, err = tr.Current(rc)
	assert.NoError(t, err)
	assert.Equal(t, map[string]int{"yes": 11, "no": 5, "maybe": 1}, totals)

	// set now to 12:00:35
	setNow(time.Date(2021, 11, 18, 12, 0, 35, 0, time.UTC))

	recordState("yes", 2)
	recordState("no", 1)

	// current window is 12:00:05 (1637236805) - 12:00:35 (1637236835) so we'll include counts from intervals..
	//  0: 12:00:30 <= t < 12:00:40 yes=2 no=1 maybe=0
	// -1: 12:00:20 <= t < 12:00:30 yes=0 no=0 maybe=0
	// -2: 12:00:10 <= t < 12:00:20 yes=3 no=1 maybe=1
	// -3: 12:00:00 <= t < 12:00:10 yes=8 no=4 maybe=0 (start of window falls halfway thru this interval so they are scaled by 0.5)
	totals, err = tr.Current(rc)
	assert.NoError(t, err)
	assert.Equal(t, map[string]int{"yes": 9, "no": 4, "maybe": 1}, totals)

	// set now to 12:00:38 - last interval is now only 20% in the window
	setNow(time.Date(2021, 11, 18, 12, 0, 38, 0, time.UTC))

	totals, err = tr.Current(rc)
	assert.NoError(t, err)
	assert.Equal(t, map[string]int{"yes": 6, "no": 2, "maybe": 1}, totals)

	// set now to 12:00:40 - that interval is now out of the window
	setNow(time.Date(2021, 11, 18, 12, 0, 40, 0, time.UTC))

	totals, err = tr.Current(rc)
	assert.NoError(t, err)
	assert.Equal(t, map[string]int{"yes": 5, "no": 2, "maybe": 1}, totals)
}

func TestBoolTracker(t *testing.T) {
	rp := assertredis.TestDB()
	rc := rp.Get()
	defer rc.Close()

	defer assertredis.FlushDB()

	defer dates.SetNowSource(dates.DefaultNowSource)
	dates.SetNowSource(dates.NewFixedNowSource(time.Date(2021, 11, 18, 12, 0, 1, 234567, time.UTC)))

	tr := redisx.NewBoolTracker("foo:2", time.Second*10, time.Second*30)

	yes, no, err := tr.Current(rc)
	assert.NoError(t, err)
	assert.Equal(t, 0, yes)
	assert.Equal(t, 0, no)

	err = tr.Record(rc, true, 1)
	require.NoError(t, err)

	tr.Record(rc, false, 1)
	tr.Record(rc, true, 1)

	assertredis.Int(t, rp, "foo:2:1637236800:true", 2)
	assertredis.Int(t, rp, "foo:2:1637236800:false", 1)

	yes, no, err = tr.Current(rc)
	assert.NoError(t, err)
	assert.Equal(t, 2, yes)
	assert.Equal(t, 1, no)

	dates.SetNowSource(dates.NewFixedNowSource(time.Date(2021, 11, 18, 12, 0, 13, 234567, time.UTC)))

	tr.Record(rc, false, 1)
	tr.Record(rc, true, 3)

	assertredis.Int(t, rp, "foo:2:1637236800:true", 2)
	assertredis.Int(t, rp, "foo:2:1637236800:false", 1)
	assertredis.Int(t, rp, "foo:2:1637236810:true", 3)
	assertredis.Int(t, rp, "foo:2:1637236810:false", 1)

	yes, no, err = tr.Current(rc)
	assert.NoError(t, err)
	assert.Equal(t, 5, yes)
	assert.Equal(t, 2, no)
}
