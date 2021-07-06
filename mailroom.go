package mailroom

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/nyaruka/gocommon/storage"
	"github.com/nyaruka/mailroom/config"
	"github.com/nyaruka/mailroom/core/queue"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/web"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/librato"
	"github.com/olivere/elastic/v7"
	"github.com/sirupsen/logrus"
)

// InitFunction is a function that will be called when mailroom starts
type InitFunction func(runtime *runtime.Runtime, wg *sync.WaitGroup, quit chan bool) error

var initFunctions = make([]InitFunction, 0)

// AddInitFunction adds an init function that will be called on startup
func AddInitFunction(initFunc InitFunction) {
	initFunctions = append(initFunctions, initFunc)
}

// TaskFunction is the function that will be called for a type of task
type TaskFunction func(ctx context.Context, rt *runtime.Runtime, task *queue.Task) error

var taskFunctions = make(map[string]TaskFunction)

// AddTaskFunction adds an task function that will be called for a type of task
func AddTaskFunction(taskType string, taskFunc TaskFunction) {
	taskFunctions[taskType] = taskFunc
}

// Mailroom is a service for handling RapidPro events
type Mailroom struct {
	ctx    context.Context
	cancel context.CancelFunc

	rt   *runtime.Runtime
	wg   *sync.WaitGroup
	quit chan bool

	batchForeman   *Foreman
	handlerForeman *Foreman

	webserver *web.Server
}

// NewMailroom creates and returns a new mailroom instance
func NewMailroom(config *config.Config) *Mailroom {
	mr := &Mailroom{
		rt:   &runtime.Runtime{Config: config},
		quit: make(chan bool),
		wg:   &sync.WaitGroup{},
	}
	mr.ctx, mr.cancel = context.WithCancel(context.Background())
	mr.batchForeman = NewForeman(mr.rt, mr.wg, queue.BatchQueue, config.BatchWorkers)
	mr.handlerForeman = NewForeman(mr.rt, mr.wg, queue.HandlerQueue, config.HandlerWorkers)

	return mr
}

// Start starts the mailroom service
func (mr *Mailroom) Start() error {
	c := mr.rt.Config

	log := logrus.WithFields(logrus.Fields{
		"state": "starting",
	})

	// parse and test our db config
	dbURL, err := url.Parse(c.DB)
	if err != nil {
		return fmt.Errorf("unable to parse DB URL '%s': %s", c.DB, err)
	}

	if dbURL.Scheme != "postgres" {
		return fmt.Errorf("invalid DB URL: '%s', only postgres is supported", c.DB)
	}

	// build our db
	db, err := sqlx.Open("postgres", c.DB)
	if err != nil {
		return fmt.Errorf("unable to open DB with config: '%s': %s", c.DB, err)
	}

	// configure our pool
	db.SetMaxIdleConns(8)
	db.SetMaxOpenConns(c.DBPoolSize)
	db.SetConnMaxLifetime(time.Minute * 30)
	mr.rt.DB = db

	// try connecting
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	err = db.PingContext(ctx)
	cancel()
	if err != nil {
		log.Error("db not reachable")
	} else {
		log.Info("db ok")
	}

	// parse and test our redis config
	redisURL, err := url.Parse(mr.rt.Config.Redis)
	if err != nil {
		return fmt.Errorf("unable to parse Redis URL '%s': %s", c.Redis, err)
	}

	// create our pool
	redisPool := &redis.Pool{
		Wait:        true,              // makes callers wait for a connection
		MaxActive:   36,                // only open this many concurrent connections at once
		MaxIdle:     4,                 // only keep up to this many idle
		IdleTimeout: 240 * time.Second, // how long to wait before reaping a connection
		Dial: func() (redis.Conn, error) {
			conn, err := redis.Dial("tcp", redisURL.Host)
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
	mr.rt.RP = redisPool

	// test our redis connection
	conn := redisPool.Get()
	defer conn.Close()
	_, err = conn.Do("PING")
	if err != nil {
		log.WithError(err).Error("redis not reachable")
	} else {
		log.Info("redis ok")
	}

	// create our storage (S3 or file system)
	if mr.rt.Config.AWSAccessKeyID != "" {
		s3Client, err := storage.NewS3Client(&storage.S3Options{
			AWSAccessKeyID:     c.AWSAccessKeyID,
			AWSSecretAccessKey: c.AWSSecretAccessKey,
			Endpoint:           c.S3Endpoint,
			Region:             c.S3Region,
			DisableSSL:         c.S3DisableSSL,
			ForcePathStyle:     c.S3ForcePathStyle,
		})
		if err != nil {
			return err
		}
		mr.rt.MediaStorage = storage.NewS3(s3Client, mr.rt.Config.S3MediaBucket, 32)
		mr.rt.SessionStorage = storage.NewS3(s3Client, mr.rt.Config.S3SessionBucket, 32)
	} else {
		mr.rt.MediaStorage = storage.NewFS("_storage")
		mr.rt.SessionStorage = storage.NewFS("_storage")
	}

	// test our media storage
	ctx, cancel = context.WithTimeout(context.Background(), time.Second*10)
	err = mr.rt.MediaStorage.Test(ctx)
	cancel()

	if err != nil {
		log.WithError(err).Error(mr.rt.MediaStorage.Name() + " media storage not available")
	} else {
		log.Info(mr.rt.MediaStorage.Name() + " media storage ok")
	}

	ctx, cancel = context.WithTimeout(context.Background(), time.Second*10)
	err = mr.rt.SessionStorage.Test(ctx)
	cancel()

	if err != nil {
		log.WithError(err).Warn(mr.rt.SessionStorage.Name() + " session storage not available")
	} else {
		log.Info(mr.rt.SessionStorage.Name() + " session storage ok")
	}

	// initialize our elastic client
	mr.rt.ES, err = newElasticClient(c.Elastic)
	if err != nil {
		log.WithError(err).Error("unable to connect to elastic, check configuration")
	} else {
		log.Info("elastic ok")
	}

	// warn if we won't be doing FCM syncing
	if c.FCMKey == "" {
		logrus.Error("fcm not configured, no syncing of android channels")
	}

	for _, initFunc := range initFunctions {
		initFunc(mr.rt, mr.wg, mr.quit)
	}

	// if we have a librato token, configure it
	if c.LibratoToken != "" {
		host, _ := os.Hostname()
		librato.Configure(c.LibratoUsername, c.LibratoToken, host, time.Second, mr.wg)
		librato.Start()
	}

	// init our foremen and start it
	mr.batchForeman.Start()
	mr.handlerForeman.Start()

	// start our web server
	mr.webserver = web.NewServer(mr.ctx, c, mr.rt.DB, mr.rt.RP, mr.rt.MediaStorage, mr.rt.ES, mr.wg)
	mr.webserver.Start()

	logrus.Info("mailroom started")

	return nil
}

// Stop stops the mailroom service
func (mr *Mailroom) Stop() error {
	logrus.Info("mailroom stopping")
	mr.batchForeman.Stop()
	mr.handlerForeman.Stop()
	librato.Stop()
	close(mr.quit)
	mr.cancel()

	// stop our web server
	mr.webserver.Stop()

	mr.wg.Wait()
	mr.rt.ES.Stop()
	logrus.Info("mailroom stopped")
	return nil
}

func newElasticClient(url string) (*elastic.Client, error) {
	// enable retrying
	backoff := elastic.NewSimpleBackoff(500, 1000, 2000)
	backoff.Jitter(true)
	retrier := elastic.NewBackoffRetrier(backoff)

	return elastic.NewClient(
		elastic.SetURL(url),
		elastic.SetSniff(false),
		elastic.SetRetrier(retrier),
	)
}
