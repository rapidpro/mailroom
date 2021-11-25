package assertredis

import (
	"fmt"

	"github.com/gomodule/redigo/redis"
)

const (
	testDBAddress = "localhost:6379"
	testDBIndex   = 0
)

// TestDB returns a redis pool to our test database
func TestDB() *redis.Pool {
	return &redis.Pool{
		Dial: func() (redis.Conn, error) {
			conn, err := redis.Dial("tcp", testDBAddress)
			if err != nil {
				return nil, err
			}
			_, err = conn.Do("SELECT", 0)
			return conn, err
		},
	}
}

// FlushDB flushes the test database
func FlushDB() {
	rc, err := redis.Dial("tcp", testDBAddress)
	if err != nil {
		panic(fmt.Sprintf("error connecting to redis db: %s", err.Error()))
	}
	rc.Do("SELECT", testDBIndex)
	_, err = rc.Do("FLUSHDB")
	if err != nil {
		panic(fmt.Sprintf("error flushing redis db: %s", err.Error()))
	}
}
