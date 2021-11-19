package redisx_test

import (
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/utils/redisx"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLocker(t *testing.T) {
	_, _, _, rp := testsuite.Get()
	rc := rp.Get()
	defer rc.Close()

	defer testsuite.Reset(testsuite.ResetRedis)

	assertKeySet := func(key string) {
		val, err := redis.String(rc.Do("GET", key))
		require.NoError(t, err)
		assert.NotEqual(t, "", val)
	}

	locker := redisx.NewLocker("test", time.Second*5)

	// acquire a lock
	lock1, err := locker.Grab(rc, time.Second)
	assert.NoError(t, err)
	assert.NotZero(t, lock1)

	assertKeySet("lock:test")

	// try to acquire the same lock, should fail
	lock2, err := locker.Grab(rc, time.Second)
	assert.NoError(t, err)
	assert.Zero(t, lock2)

	// should succeed if we wait longer
	lock3, err := locker.Grab(rc, time.Second*6)
	assert.NoError(t, err)
	assert.NotZero(t, lock3)
	assert.NotEqual(t, lock1, lock3)

	// extend the lock
	err = locker.Extend(rc, lock3, time.Second*10)
	assert.NoError(t, err)

	// trying to grab it should fail with a 5 second timeout
	lock4, err := locker.Grab(rc, time.Second*5)
	assert.NoError(t, err)
	assert.Zero(t, lock4)

	// try to release the lock with wrong value
	err = locker.Release(rc, "2352")
	assert.NoError(t, err)

	// no error but also dooesn't release the lock
	assertKeySet("lock:test")

	// release the lock
	err = locker.Release(rc, lock3)
	assert.NoError(t, err)

	testsuite.AssertRedisNotExists(t, rp, "lock:test")

	// new grab should work
	lock5, err := locker.Grab(rc, time.Second*5)
	assert.NoError(t, err)
	assert.NotZero(t, lock5)

	assertKeySet("lock:test")
}
