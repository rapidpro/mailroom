package locker_test

import (
	"testing"
	"time"

	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/utils/locker"

	"github.com/stretchr/testify/assert"
)

func TestLocker(t *testing.T) {
	_, _, _, rp := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetRedis)

	// acquire a lock, but have it expire in 5 seconds
	v1, err := locker.GrabLock(rp, "test", time.Second*5, time.Second)
	assert.NoError(t, err)
	assert.NotZero(t, v1)

	// try to acquire the same lock, should fail
	v2, err := locker.GrabLock(rp, "test", time.Second*5, time.Second)
	assert.NoError(t, err)
	assert.Zero(t, v2)

	// should succeed if we wait longer
	v3, err := locker.GrabLock(rp, "test", time.Second*5, time.Second*5)
	assert.NoError(t, err)
	assert.NotZero(t, v3)
	assert.NotEqual(t, v1, v3)

	// extend the lock
	err = locker.ExtendLock(rp, "test", v3, time.Second*10)
	assert.NoError(t, err)

	// trying to grab it should fail with a 5 second timeout
	v4, err := locker.GrabLock(rp, "test", time.Second*5, time.Second*5)
	assert.NoError(t, err)
	assert.Zero(t, v4)

	// return the lock
	err = locker.ReleaseLock(rp, "test", v3)
	assert.NoError(t, err)

	// new grab should work
	v5, err := locker.GrabLock(rp, "test", time.Second*5, time.Second*5)
	assert.NoError(t, err)
	assert.NotZero(t, v5)
}
