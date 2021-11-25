package assertredis

import (
	"sort"
	"testing"

	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/assert"
)

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

// IntSet asserts that the given key contains a set with the given int values
func IntSet(t *testing.T, rp *redis.Pool, key string, expected []int, msgAndArgs ...interface{}) {
	actual, err := redis.Ints(do(rp, "SMEMBERS", key))

	sort.Ints(actual)

	assert.NoError(t, err)
	assert.Equal(t, expected, actual, msgAndArgs...)
}

// StringSet asserts that the given key contains a set with the given string values
func StringSet(t *testing.T, rp *redis.Pool, key string, expected []string, msgAndArgs ...interface{}) {
	actual, err := redis.Strings(do(rp, "SMEMBERS", key))

	sort.Strings(actual)

	assert.NoError(t, err)
	assert.Equal(t, expected, actual, msgAndArgs...)
}

func do(rp *redis.Pool, commandName string, args ...interface{}) (reply interface{}, err error) {
	rc := rp.Get()
	defer rc.Close()

	return rc.Do(commandName, args...)
}
