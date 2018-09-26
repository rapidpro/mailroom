package mailroom

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/librato"
	_ "github.com/nyaruka/mailroom/handlers"
	"github.com/nyaruka/mailroom/queue"
	"github.com/sirupsen/logrus"
)

// InitFunction is a function that will be called when mailroom starts
type InitFunction func(mr *Mailroom) error

var initFunctions = make([]InitFunction, 0)

// AddInitFunction adds an init function that will be called on startup
func AddInitFunction(initFunc InitFunction) {
	initFunctions = append(initFunctions, initFunc)
}

// TaskFunction is the function that will be called for a type of task
type TaskFunction func(mr *Mailroom, task *queue.Task) error

var taskFunctions = make(map[string]TaskFunction)

// AddTaskFunction adds an task function that will be called for a type of task
func AddTaskFunction(taskType string, taskFunc TaskFunction) {
	taskFunctions[taskType] = taskFunc
}

// Mailroom is a service for handling RapidPro events
type Mailroom struct {
	Config    *Config
	DB        *sqlx.DB
	RedisPool *redis.Pool
	Quit      chan bool
	CTX       context.Context
	Cancel    context.CancelFunc
	WaitGroup *sync.WaitGroup
	foreman   *Foreman
}

// NewMailroom creates and returns a new mailroom instance
func NewMailroom(config *Config) *Mailroom {
	mr := &Mailroom{
		Config:    config,
		Quit:      make(chan bool),
		WaitGroup: &sync.WaitGroup{},
	}
	mr.CTX, mr.Cancel = context.WithCancel(context.Background())
	mr.foreman = NewForeman(mr, "batch", config.BatchWorkers)

	return mr
}

// Start starts the mailroom service
func (mr *Mailroom) Start() error {
	log := logrus.WithFields(logrus.Fields{
		"state": "starting",
	})

	// parse and test our db config
	dbURL, err := url.Parse(mr.Config.DB)
	if err != nil {
		return fmt.Errorf("unable to parse DB URL '%s': %s", mr.Config.DB, err)
	}

	if dbURL.Scheme != "postgres" {
		return fmt.Errorf("invalid DB URL: '%s', only postgres is supported", mr.Config.DB)
	}

	// build our db
	db, err := sqlx.Open("postgres", mr.Config.DB)
	if err != nil {
		return fmt.Errorf("unable to open DB with config: '%s': %s", mr.Config.DB, err)
	}

	// configure our pool
	mr.DB = db
	mr.DB.SetMaxIdleConns(4)
	mr.DB.SetMaxOpenConns(8)

	// try connecting
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	err = mr.DB.PingContext(ctx)
	cancel()
	if err != nil {
		log.Error("db not reachable")
	} else {
		log.Info("db ok")
	}

	// parse and test our redis config
	redisURL, err := url.Parse(mr.Config.Redis)
	if err != nil {
		return fmt.Errorf("unable to parse Redis URL '%s': %s", mr.Config.Redis, err)
	}

	// create our pool
	redisPool := &redis.Pool{
		Wait:        true,                 // makes callers wait for a connection
		MaxActive:   mr.Config.DBPoolSize, // only open this many concurrent connections at once
		MaxIdle:     2,                    // only keep up to this many idle
		IdleTimeout: 240 * time.Second,    // how long to wait before reaping a connection
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
	mr.RedisPool = redisPool

	// test our redis connection
	conn := redisPool.Get()
	defer conn.Close()
	_, err = conn.Do("PING")
	if err != nil {
		log.WithError(err).Error("redis not reachable")
	} else {
		log.Info("redis ok")
	}

	for _, initFunc := range initFunctions {
		initFunc(mr)
	}

	// if we have a librato token, configure it
	if mr.Config.LibratoToken != "" {
		host, _ := os.Hostname()
		librato.Configure(mr.Config.LibratoUsername, mr.Config.LibratoToken, host, time.Second*5, mr.WaitGroup)
		librato.Start()
	}

	// init our foreman and start it
	mr.foreman.Start()

	logrus.Info("mailroom started")
	return nil
}

// Stop stops the mailroom service
func (mr *Mailroom) Stop() error {
	logrus.Info("mailroom stopping")
	mr.foreman.Stop()
	librato.Stop()
	close(mr.Quit)
	mr.Cancel()
	mr.WaitGroup.Wait()
	logrus.Info("mailroom stopped")
	return nil
}
