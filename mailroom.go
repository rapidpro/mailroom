package mailroom

import (
	"context"
	"database/sql"
	"log/slog"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/gocommon/analytics"
	"github.com/nyaruka/gocommon/storage"
	"github.com/nyaruka/mailroom/core/queue"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/utils/cron"
	"github.com/nyaruka/mailroom/web"
	"github.com/olivere/elastic/v7"
	"github.com/pkg/errors"
)

// InitFunction is a function that will be called when mailroom starts
type InitFunction func(*runtime.Runtime, *sync.WaitGroup, chan bool) error

var initFunctions = make([]InitFunction, 0)

func addInitFunction(initFunc InitFunction) {
	initFunctions = append(initFunctions, initFunc)
}

// RegisterCron registers a new cron function to run every interval
func RegisterCron(name string, allInstances bool, fn cron.Function, next func(time.Time) time.Time) {
	addInitFunction(func(rt *runtime.Runtime, wg *sync.WaitGroup, quit chan bool) error {
		cron.Start(rt, wg, name, allInstances, fn, next, time.Minute*5, quit)
		return nil
	})
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
func NewMailroom(config *runtime.Config) *Mailroom {
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

	log := slog.With("comp", "mailroom")

	var err error
	_, mr.rt.DB, err = openAndCheckDBConnection(c.DB, c.DBPoolSize)
	if err != nil {
		log.Error("db not reachable", "error", err)
	} else {
		log.Info("db ok")
	}

	if c.ReadonlyDB != "" {
		mr.rt.ReadonlyDB, _, err = openAndCheckDBConnection(c.ReadonlyDB, c.DBPoolSize)
		if err != nil {
			log.Error("readonly db not reachable", "error", err)
		} else {
			log.Info("readonly db ok")
		}
	} else {
		// if readonly DB not specified, just use default DB again
		mr.rt.ReadonlyDB = mr.rt.DB.DB
		log.Warn("no distinct readonly db configured")
	}

	mr.rt.RP, err = openAndCheckRedisPool(c.Redis)
	if err != nil {
		log.Error("redis not reachable", "error", err)
	} else {
		log.Info("redis ok")
	}

	// create our storage (S3 or file system)
	if mr.rt.Config.AWSAccessKeyID != "" || mr.rt.Config.AWSUseCredChain {
		s3config := &storage.S3Options{
			Endpoint:       c.S3Endpoint,
			Region:         c.S3Region,
			DisableSSL:     c.S3DisableSSL,
			ForcePathStyle: c.S3ForcePathStyle,
			MaxRetries:     3,
		}
		if mr.rt.Config.AWSAccessKeyID != "" && !mr.rt.Config.AWSUseCredChain {
			s3config.AWSAccessKeyID = c.AWSAccessKeyID
			s3config.AWSSecretAccessKey = c.AWSSecretAccessKey
		}
		s3Client, err := storage.NewS3Client(s3config)
		if err != nil {
			return err
		}
		mr.rt.AttachmentStorage = storage.NewS3(s3Client, mr.rt.Config.S3AttachmentsBucket, c.S3Region, s3.BucketCannedACLPublicRead, 32)
		mr.rt.SessionStorage = storage.NewS3(s3Client, mr.rt.Config.S3SessionsBucket, c.S3Region, s3.ObjectCannedACLPrivate, 32)
		mr.rt.LogStorage = storage.NewS3(s3Client, mr.rt.Config.S3LogsBucket, c.S3Region, s3.ObjectCannedACLPrivate, 32)
	} else {
		mr.rt.AttachmentStorage = storage.NewFS("_storage/attachments", 0766)
		mr.rt.SessionStorage = storage.NewFS("_storage/sessions", 0766)
		mr.rt.LogStorage = storage.NewFS("_storage/logs", 0766)
	}

	// check our storages
	if err := checkStorage(mr.rt.AttachmentStorage); err != nil {
		log.Error(mr.rt.AttachmentStorage.Name()+" attachment storage not available", "error", err)
	} else {
		log.Info(mr.rt.AttachmentStorage.Name() + " attachment storage ok")
	}
	if err := checkStorage(mr.rt.SessionStorage); err != nil {
		log.Error(mr.rt.SessionStorage.Name()+" session storage not available", "error", err)
	} else {
		log.Info(mr.rt.SessionStorage.Name() + " session storage ok")
	}
	if err := checkStorage(mr.rt.LogStorage); err != nil {
		log.Error(mr.rt.LogStorage.Name()+" log storage not available", "error", err)
	} else {
		log.Info(mr.rt.LogStorage.Name() + " log storage ok")
	}

	// initialize our elastic client
	mr.rt.ES, err = newElasticClient(c.Elastic, c.ElasticUsername, c.ElasticPassword)
	if err != nil {
		log.Error("elastic search not available", "error", err)
	} else {
		log.Info("elastic ok")
	}

	// warn if we won't be doing FCM syncing
	if c.FCMKey == "" {
		log.Warn("fcm not configured, no android syncing")
	}

	for _, initFunc := range initFunctions {
		initFunc(mr.rt, mr.wg, mr.quit)
	}

	// if we have a librato token, configure it
	if c.LibratoToken != "" {
		analytics.RegisterBackend(analytics.NewLibrato(c.LibratoUsername, c.LibratoToken, c.InstanceName, time.Second, mr.wg))
	}

	analytics.Start()

	// init our foremen and start it
	mr.batchForeman.Start()
	mr.handlerForeman.Start()

	// start our web server
	mr.webserver = web.NewServer(mr.ctx, mr.rt, mr.wg)
	mr.webserver.Start()

	log.Info("mailroom started", "domain", c.Domain)

	return nil
}

// Stop stops the mailroom service
func (mr *Mailroom) Stop() error {
	log := slog.With("comp", "mailroom")
	log.Info("mailroom stopping")

	mr.batchForeman.Stop()
	mr.handlerForeman.Stop()
	analytics.Stop()
	close(mr.quit)
	mr.cancel()

	// stop our web server
	mr.webserver.Stop()

	mr.wg.Wait()

	// stop ES client if we have one
	if mr.rt.ES != nil {
		mr.rt.ES.Stop()
	}

	log.Info("mailroom stopped")
	return nil
}

func openAndCheckDBConnection(url string, maxOpenConns int) (*sql.DB, *sqlx.DB, error) {
	db, err := sqlx.Open("postgres", url)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "unable to open database connection: '%s'", url)
	}

	// configure our pool
	db.SetMaxIdleConns(8)
	db.SetMaxOpenConns(maxOpenConns)
	db.SetConnMaxLifetime(time.Minute * 30)

	// ping database...
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	err = db.PingContext(ctx)
	cancel()

	return db.DB, db, err
}

func openAndCheckRedisPool(redisUrl string) (*redis.Pool, error) {
	redisURL, _ := url.Parse(redisUrl)

	rp := &redis.Pool{
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

	// test the connection
	conn := rp.Get()
	defer conn.Close()
	_, err := conn.Do("PING")

	return rp, err
}

func newElasticClient(url string, username string, password string) (*elastic.Client, error) {
	// enable retrying
	backoff := elastic.NewSimpleBackoff(500, 1000, 2000)
	backoff.Jitter(true)
	retrier := elastic.NewBackoffRetrier(backoff)

	return elastic.NewClient(
		elastic.SetURL(url),
		elastic.SetSniff(false),
		elastic.SetRetrier(retrier),
		elastic.SetBasicAuth(username, password),
	)
}

func checkStorage(s storage.Storage) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	err := s.Test(ctx)
	cancel()
	return err
}
