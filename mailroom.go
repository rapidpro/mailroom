package mailroom

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"
)

// Mailroom is a service for handling RapidPro events
type Mailroom struct {
	config    *Config
	db        *sqlx.DB
	redisPool *redis.Pool
	quit      chan bool
	ctx       context.Context
	cancel    context.CancelFunc
}

// NewMailroom creates and returns a new mailroom instance
func NewMailroom(config *Config) *Mailroom {
	mr := &Mailroom{
		config: config,
		quit:   make(chan bool),
	}
	mr.ctx, mr.cancel = context.WithCancel(context.Background())

	return mr
}

// Start starts the mailroom service
func (mr *Mailroom) Start() error {
	log := logrus.WithFields(logrus.Fields{
		"state": "starting",
	})

	// parse and test our db config
	dbURL, err := url.Parse(mr.config.DB)
	if err != nil {
		return fmt.Errorf("unable to parse DB URL '%s': %s", mr.config.DB, err)
	}

	if dbURL.Scheme != "postgres" {
		return fmt.Errorf("invalid DB URL: '%s', only postgres is supported", mr.config.DB)
	}

	// build our db
	db, err := sqlx.Open("postgres", mr.config.DB)
	if err != nil {
		return fmt.Errorf("unable to open DB with config: '%s': %s", mr.config.DB, err)
	}

	// configure our pool
	mr.db = db
	mr.db.SetMaxIdleConns(4)
	mr.db.SetMaxOpenConns(16)

	// try connecting
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	err = mr.db.PingContext(ctx)
	cancel()
	if err != nil {
		log.Error("db not reachable")
	} else {
		log.Info("db ok")
	}

	// parse and test our redis config
	redisURL, err := url.Parse(mr.config.Redis)
	if err != nil {
		return fmt.Errorf("unable to parse Redis URL '%s': %s", mr.config.Redis, err)
	}

	// create our pool
	redisPool := &redis.Pool{
		Wait:        true,              // makes callers wait for a connection
		MaxActive:   8,                 // only open this many concurrent connections at once
		MaxIdle:     4,                 // only keep up to this many idle
		IdleTimeout: 240 * time.Second, // how long to wait before reaping a connection
		Dial: func() (redis.Conn, error) {
			conn, err := redis.Dial("tcp", fmt.Sprintf("%s", redisURL.Host))
			if err != nil {
				return nil, err
			}

			// send auth if required
			if redisURL.User != nil {
				pass, authRequired := redisURL.User.Password()
				if authRequired {
					if _, err := conn.Do("AUTH", pass); err != nil {
						conn.Close()
						return nil, err
					}
				}
			}

			// switch to the right DB
			_, err = conn.Do("SELECT", strings.TrimLeft(redisURL.Path, "/"))
			return conn, err
		},
	}
	mr.redisPool = redisPool

	// test our redis connection
	conn := redisPool.Get()
	defer conn.Close()
	_, err = conn.Do("PING")
	if err != nil {
		log.WithError(err).Error("redis not reachable")
	} else {
		log.Info("redis ok")
	}

	go startExpiring(mr)

	logrus.Info("mailroom started")
	return nil
}

// Stop stops the mailroom service
func (mr *Mailroom) Stop() error {
	logrus.Info("mailroom stopping")
	close(mr.quit)
	mr.cancel()
	logrus.Info("mailroom stopped")
	return nil
}
