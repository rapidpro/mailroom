package assertredis

import (
	"testing"

	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/assert"
)

// Keys asserts that only the given keys exist
func Keys(t *testing.T, rp *redis.Pool, expected []string, msgAndArgs ...interface{}) {
	actual, err := redis.Strings(do(rp, "KEYS", "*"))

	assert.NoError(t, err)
	assert.ElementsMatch(t, expected, actual, msgAndArgs...)
}

// Exists asserts that the given key exists
func Exists(t *testing.T, rp *redis.Pool, key string, msgAndArgs ...interface{}) {
	exists, err := redis.Int(do(rp, "EXISTS", key))

	assert.NoError(t, err)
	assert.Equal(t, 1, exists, msgAndArgs...)
}

// NotExists asserts that the given key does not exist
func NotExists(t *testing.T, rp *redis.Pool, key string, msgAndArgs ...interface{}) {
	exists, err := redis.Int(do(rp, "EXISTS", key))

	assert.NoError(t, err)
	assert.Equal(t, 0, exists, msgAndArgs...)
}

// Int asserts that the given key contains the given int value
func Int(t *testing.T, rp *redis.Pool, key string, expected int, msgAndArgs ...interface{}) {
	actual, err := redis.Int(do(rp, "GET", key))

	assert.NoError(t, err)
	assert.Equal(t, expected, actual, msgAndArgs...)
}

// String asserts that the given key contains the given string value
func String(t *testing.T, rp *redis.Pool, key string, expected string, msgAndArgs ...interface{}) {
	actual, err := redis.String(do(rp, "GET", key))

	assert.NoError(t, err)
	assert.Equal(t, expected, actual, msgAndArgs...)
}

// Set asserts that the given key contains a set with the given values
func Set(t *testing.T, rp *redis.Pool, key string, expected []string, msgAndArgs ...interface{}) {
	actual, err := redis.Strings(do(rp, "SMEMBERS", key))

	assert.NoError(t, err)
	assert.ElementsMatch(t, expected, actual, msgAndArgs...)
}

// Hash asserts that the given key contains a hash with the given values
func Hash(t *testing.T, rp *redis.Pool, key string, expected map[string]string, msgAndArgs ...interface{}) {
	actual, err := redis.StringMap(do(rp, "HGETALL", key))

	assert.NoError(t, err)
	assert.Equal(t, expected, actual, msgAndArgs...)
}

func do(rp *redis.Pool, commandName string, args ...interface{}) (reply interface{}, err error) {
	rc := rp.Get()
	defer rc.Close()

	return rc.Do(commandName, args...)
}
