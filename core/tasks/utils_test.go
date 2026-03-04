package tasks_test

import (
	"testing"
	"time"

	valkey "github.com/gomodule/redigo/redis"
	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/stretchr/testify/assert"
)

func TestCounter(t *testing.T) {
	ctx, rt := testsuite.Runtime(t)
	vc := rt.VK.Get()
	defer vc.Close()

	defer testsuite.Reset(t, rt, testsuite.ResetValkey)

	counter := tasks.NewCounter("test_counter", time.Minute)

	// init counter with 3 batches
	err := counter.Init(ctx, rt.VK, 3)
	assert.NoError(t, err)

	val, err := valkey.Int(vc.Do("GET", "test_counter"))
	assert.NoError(t, err)
	assert.Equal(t, 3, val)

	ttl, err := valkey.Int(vc.Do("TTL", "test_counter"))
	assert.NoError(t, err)
	assert.Greater(t, ttl, 0)

	// first two calls to Done should return false
	done, err := counter.Done(ctx, rt.VK)
	assert.NoError(t, err)
	assert.False(t, done)

	done, err = counter.Done(ctx, rt.VK)
	assert.NoError(t, err)
	assert.False(t, done)

	// last call should return true
	done, err = counter.Done(ctx, rt.VK)
	assert.NoError(t, err)
	assert.True(t, done)

	// key should still exist with a TTL (not orphaned)
	ttl, err = valkey.Int(vc.Do("TTL", "test_counter"))
	assert.NoError(t, err)
	assert.Greater(t, ttl, 0)
}

func TestCounterDoneSetsExpiry(t *testing.T) {
	ctx, rt := testsuite.Runtime(t)
	vc := rt.VK.Get()
	defer vc.Close()

	defer testsuite.Reset(t, rt, testsuite.ResetValkey)

	// simulate a key that was created by DECR without a TTL (the fragility we're fixing)
	vc.Do("SET", "test_counter_orphan", 1)

	ttl, err := valkey.Int(vc.Do("TTL", "test_counter_orphan"))
	assert.NoError(t, err)
	assert.Equal(t, -1, ttl) // no expiry

	counter := tasks.NewCounter("test_counter_orphan", time.Minute)

	done, err := counter.Done(ctx, rt.VK)
	assert.NoError(t, err)
	assert.True(t, done)

	// TTL should now be set
	ttl, err = valkey.Int(vc.Do("TTL", "test_counter_orphan"))
	assert.NoError(t, err)
	assert.Greater(t, ttl, 0)
}
