package tasks

import (
	"context"
	"time"

	valkey "github.com/gomodule/redigo/redis"
)

// Counter is a Valkey-backed counter for tracking batch completion. It is initialized with the number of batches via
// Init and then Done is called as each batch completes. Done returns true for the last batch to complete.
type Counter struct {
	key string
	ttl time.Duration
}

// NewCounter creates a new counter with the given key and TTL.
func NewCounter(key string, ttl time.Duration) *Counter {
	return &Counter{key: key, ttl: ttl}
}

// Init sets the counter to the given value with the configured TTL.
func (c *Counter) Init(ctx context.Context, vk *valkey.Pool, val int) error {
	vc := vk.Get()
	defer vc.Close()

	_, err := valkey.DoContext(vc, ctx, "SET", c.key, val, "EX", int(c.ttl.Seconds()))
	return err
}

// Done decrements the counter by 1 and returns true if the counter has reached zero, i.e. all batches are done.
// The TTL is always reset to prevent orphaned keys.
func (c *Counter) Done(ctx context.Context, vk *valkey.Pool) (bool, error) {
	val, err := c.decrement(ctx, vk, -1)
	if err != nil {
		return false, err
	}

	return val <= 0, nil
}

// Value returns the current counter value, or 0 if the key doesn't exist.
func (c *Counter) Value(ctx context.Context, vk *valkey.Pool) (int, error) {
	vc := vk.Get()
	defer vc.Close()

	val, err := valkey.Int(valkey.DoContext(vc, ctx, "GET", c.key))
	if err == valkey.ErrNil {
		return 0, nil
	}
	return val, err
}

// Clear deletes the counter key.
func (c *Counter) Clear(ctx context.Context, vk *valkey.Pool) error {
	vc := vk.Get()
	defer vc.Close()

	_, err := valkey.DoContext(vc, ctx, "DEL", c.key)
	return err
}

func (c *Counter) decrement(ctx context.Context, vk *valkey.Pool, by int) (int, error) {
	vc := vk.Get()
	defer vc.Close()

	return valkey.Int(counterDecr.DoContext(ctx, vc, c.key, by, int(c.ttl.Seconds())))
}

var counterDecr = valkey.NewScript(1, `
local val = redis.call('INCRBY', KEYS[1], ARGV[1])
redis.call('EXPIRE', KEYS[1], ARGV[2])
return val
`)
