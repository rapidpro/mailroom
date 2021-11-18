package redisx_test

import (
	"testing"
	"time"

	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/utils/redisx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBoolTracker(t *testing.T) {
	_, _, _, rp := testsuite.Get()
	rc := rp.Get()
	defer rc.Close()

	defer testsuite.Reset(testsuite.ResetRedis)

	defer dates.SetNowSource(dates.DefaultNowSource)
	dates.SetNowSource(dates.NewFixedNowSource(time.Date(2021, 11, 18, 12, 0, 1, 234567, time.UTC)))

	tr := redisx.NewBoolTracker("foo:1", time.Second*10, time.Second*30)

	yes, no, err := tr.Current(rc)
	assert.NoError(t, err)
	assert.Equal(t, 0, yes)
	assert.Equal(t, 0, no)

	err = tr.Record(rc, true)
	require.NoError(t, err)

	tr.Record(rc, false)
	tr.Record(rc, true)

	testsuite.AssertRedisInt(t, rp, "foo:1:1637236800:true", 2)
	testsuite.AssertRedisInt(t, rp, "foo:1:1637236800:false", 1)

	yes, no, err = tr.Current(rc)
	assert.NoError(t, err)
	assert.Equal(t, 2, yes)
	assert.Equal(t, 1, no)

	dates.SetNowSource(dates.NewFixedNowSource(time.Date(2021, 11, 18, 12, 0, 13, 234567, time.UTC)))

	tr.Record(rc, false)
	tr.Record(rc, true)
	tr.Record(rc, true)
	tr.Record(rc, true)

	testsuite.AssertRedisInt(t, rp, "foo:1:1637236800:true", 2)
	testsuite.AssertRedisInt(t, rp, "foo:1:1637236800:false", 1)
	testsuite.AssertRedisInt(t, rp, "foo:1:1637236810:true", 3)
	testsuite.AssertRedisInt(t, rp, "foo:1:1637236810:false", 1)

	yes, no, err = tr.Current(rc)
	assert.NoError(t, err)
	assert.Equal(t, 5, yes)
	assert.Equal(t, 2, no)
}
