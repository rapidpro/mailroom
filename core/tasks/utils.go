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
	vc := vk.Get()
	defer vc.Close()

	vc.Send("DECR", c.key)
	vc.Send("EXPIRE", c.key, int(c.ttl.Seconds()))
	if err := vc.Flush(); err != nil {
		return false, err
	}

	val, err := valkey.Int(vc.Receive())
	if err != nil {
		return false, err
	}
	if _, err := vc.Receive(); err != nil { // discard EXPIRE response
		return false, err
	}

	return val <= 0, nil
}
