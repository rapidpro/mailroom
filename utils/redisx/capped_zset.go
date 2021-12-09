package redisx

import (
	"time"

	"github.com/gomodule/redigo/redis"
)

// CappedZSet is a sorted set but enforces a cap on size
type CappedZSet struct {
	key    string
	cap    int
	expire time.Duration
}

// NewCappedZSet creates a new capped sorted set
func NewCappedZSet(key string, cap int, expire time.Duration) *CappedZSet {
	return &CappedZSet{key: key, cap: cap, expire: expire}
}

var czsetAddScript = redis.NewScript(1, `
local key, score, member, cap, expire = KEYS[1], ARGV[1], ARGV[2], tonumber(ARGV[3]), ARGV[4]

redis.call("ZADD", key, score, member)
redis.call("EXPIRE", key, expire)
local newSize = redis.call("ZCARD", key)

if newSize > cap then
	redis.call("ZREMRANGEBYRANK", key, 0, (newSize - cap) - 1)
end
`)

// Add adds an element to the set, if its score puts in the top `cap` members
func (z *CappedZSet) Add(rc redis.Conn, member string, score float64) error {
	_, err := czsetAddScript.Do(rc, z.key, score, member, z.cap, int(z.expire/time.Second))
	return err
}

// Card returns the cardinality of the set
func (z *CappedZSet) Card(rc redis.Conn) (int, error) {
	return redis.Int(rc.Do("ZCARD", z.key))
}

// Members returns all members of the set, ordered by ascending rank
func (z *CappedZSet) Members(rc redis.Conn) ([]string, []float64, error) {
	return StringsWithScores(rc.Do("ZRANGE", z.key, 0, -1, "WITHSCORES"))
}
