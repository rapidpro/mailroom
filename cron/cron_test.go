package cron

import (
	"sync"
	"testing"
	"time"

	"github.com/nyaruka/mailroom/testsuite"
	"github.com/stretchr/testify/assert"
)

func TestCron(t *testing.T) {
	testsuite.ResetRP()
	rp := testsuite.RP()
	rc := testsuite.RC()
	defer rc.Close()

	mutex := sync.RWMutex{}
	fired := 0
	quit := make(chan bool)

	// our cron worker is just going to increment an int on every fire
	increment := func(lockName string, lockValue string) error {
		mutex.Lock()
		fired++
		mutex.Unlock()
		return nil
	}

	StartCron(quit, rp, "test", time.Millisecond*100, increment)

	// wait a bit, should only have fired three times (initial time + three timeouts)
	time.Sleep(time.Millisecond * 320)
	assert.Equal(t, 4, fired)

	// grab our lock
	locked, err := GrabLock(rc, "test_lock", "123456789", 300)
	assert.True(t, locked)
	assert.NoError(t, err)

	// cant lock with a different key
	locked, err = GrabLock(rc, "test_lock", "different", 300)
	assert.False(t, locked)
	assert.NoError(t, err)

	// extend our lock
	err = ExtendLock(rc, "test_lock", "123456789", 300)
	assert.NoError(t, err)

	// sleep some more
	time.Sleep(time.Millisecond * 300)

	// our # of fires should be the same
	assert.Equal(t, 4, fired)

	// release the lock
	err = ReleaseLock(rc, "test_lock", "123456789")
	assert.NoError(t, err)

	// sleep some more
	time.Sleep(time.Millisecond * 300)

	// should have incremented three more times
	assert.Equal(t, 7, fired)

	close(quit)
}

func TestNextFire(t *testing.T) {
	tcs := []struct {
		last     time.Time
		interval time.Duration
		next     time.Time
	}{
		{time.Date(2000, 1, 1, 1, 1, 4, 0, time.UTC), time.Minute, time.Date(2000, 1, 1, 1, 2, 1, 0, time.UTC)},
		{time.Date(2000, 1, 1, 1, 1, 44, 0, time.UTC), time.Minute, time.Date(2000, 1, 1, 1, 2, 1, 0, time.UTC)},
		{time.Date(2000, 1, 1, 1, 1, 1, 100, time.UTC), time.Millisecond * 150, time.Date(2000, 1, 1, 1, 1, 1, 150000100, time.UTC)},
		{time.Date(2000, 1, 1, 2, 6, 1, 0, time.UTC), time.Minute * 10, time.Date(2000, 1, 1, 2, 16, 1, 0, time.UTC)},
		{time.Date(2000, 1, 1, 1, 1, 4, 0, time.UTC), time.Second * 15, time.Date(2000, 1, 1, 1, 1, 15, 0, time.UTC)},
	}

	for _, tc := range tcs {
		next := nextFire(tc.last, tc.interval)
		assert.Equal(t, tc.next, next)
	}
}
