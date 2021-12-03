package redisx_test

import (
	"testing"
	"time"

	"github.com/nyaruka/mailroom/utils/redisx"
	"github.com/nyaruka/mailroom/utils/redisx/assertredis"
	"github.com/stretchr/testify/assert"
)

func TestLocker(t *testing.T) {
	rp := assertredis.TestDB()

	defer assertredis.FlushDB()

	locker := redisx.NewLocker("test", time.Second*5)

	// acquire a lock
	lock1, err := locker.Grab(rp, time.Second)
	assert.NoError(t, err)
	assert.NotZero(t, lock1)

	assertredis.Exists(t, rp, "test")

	// try to acquire the same lock, should fail
	lock2, err := locker.Grab(rp, time.Second)
	assert.NoError(t, err)
	assert.Zero(t, lock2)

	// should succeed if we wait longer
	lock3, err := locker.Grab(rp, time.Second*6)
	assert.NoError(t, err)
	assert.NotZero(t, lock3)
	assert.NotEqual(t, lock1, lock3)

	// extend the lock
	err = locker.Extend(rp, lock3, time.Second*10)
	assert.NoError(t, err)

	// trying to grab it should fail with a 5 second timeout
	lock4, err := locker.Grab(rp, time.Second*5)
	assert.NoError(t, err)
	assert.Zero(t, lock4)

	// try to release the lock with wrong value
	err = locker.Release(rp, "2352")
	assert.NoError(t, err)

	// no error but also dooesn't release the lock
	assertredis.Exists(t, rp, "test")

	// release the lock
	err = locker.Release(rp, lock3)
	assert.NoError(t, err)

	assertredis.NotExists(t, rp, "test")

	// new grab should work
	lock5, err := locker.Grab(rp, time.Second*5)
	assert.NoError(t, err)
	assert.NotZero(t, lock5)

	assertredis.Exists(t, rp, "test")
}
