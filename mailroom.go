package mailroom

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/nyaruka/mailroom/config"
	"github.com/nyaruka/mailroom/queue"
	"github.com/nyaruka/mailroom/s3utils"
	"github.com/nyaruka/mailroom/web"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/librato"
	"github.com/olivere/elastic"
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
type TaskFunction func(ctx context.Context, mr *Mailroom, task *queue.Task) error

var taskFunctions = make(map[string]TaskFunction)

// AddTaskFunction adds an task function that will be called for a type of task
func AddTaskFunction(taskType string, taskFunc TaskFunction) {
	taskFunctions[taskType] = taskFunc
}

// Mailroom is a service for handling RapidPro events
type Mailroom struct {
	Config        *config.Config
	DB            *sqlx.DB
	RP            *redis.Pool
	ElasticClient *elastic.Client
	S3Client      s3iface.S3API

	Quit      chan bool
	CTX       context.Context
	Cancel    context.CancelFunc
	WaitGroup *sync.WaitGroup

	batchForeman   *Foreman
	handlerForeman *Foreman

	webserver *web.Server
}

// NewMailroom creates and returns a new mailroom instance
func NewMailroom(config *config.Config) *Mailroom {
	mr := &Mailroom{
		Config:    config,
		Quit:      make(chan bool),
		WaitGroup: &sync.WaitGroup{},
	}
	mr.CTX, mr.Cancel = context.WithCancel(context.Background())
	mr.batchForeman = NewForeman(mr, queue.BatchQueue, config.BatchWorkers)
	mr.handlerForeman = NewForeman(mr, queue.HandlerQueue, config.HandlerWorkers)

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
	mr.DB.SetMaxIdleConns(8)
	mr.DB.SetMaxOpenConns(mr.Config.DBPoolSize)
	mr.DB.SetConnMaxLifetime(time.Minute * 30)

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
		Wait:        true,              // makes callers wait for a connection
		MaxActive:   36,                // only open this many concurrent connections at once
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
	mr.RP = redisPool

	// test our redis connection
	conn := redisPool.Get()
	defer conn.Close()
	_, err = conn.Do("PING")
	if err != nil {
		log.WithError(err).Error("redis not reachable")
	} else {
		log.Info("redis ok")
	}

	// create our s3 client
	s3Session, err := session.NewSession(&aws.Config{
		Credentials:      credentials.NewStaticCredentials(mr.Config.AWSAccessKeyID, mr.Config.AWSSecretAccessKey, ""),
		Endpoint:         aws.String(mr.Config.S3Endpoint),
		Region:           aws.String(mr.Config.S3Region),
		DisableSSL:       aws.Bool(mr.Config.S3DisableSSL),
		S3ForcePathStyle: aws.Bool(mr.Config.S3ForcePathStyle),
	})
	if err != nil {
		return err
	}
	mr.S3Client = s3.New(s3Session)

	// test out our S3 credentials
	err = s3utils.TestS3(mr.S3Client, mr.Config.S3MediaBucket)
	if err != nil {
		log.WithError(err).Error("s3 bucket not reachable")
	} else {
		log.Info("s3 bucket ok")
	}

	// initialize our elastic client
	mr.ElasticClient, err = elastic.NewClient(
		elastic.SetURL(mr.Config.Elastic),
		elastic.SetSniff(false),
	)
	if err != nil {
		log.WithError(err).Error("unable to connect to elastic, check configuration")
	} else {
		log.Info("elastic ok")
	}

	for _, initFunc := range initFunctions {
		initFunc(mr)
	}

	// if we have a librato token, configure it
	if mr.Config.LibratoToken != "" {
		host, _ := os.Hostname()
		librato.Configure(mr.Config.LibratoUsername, mr.Config.LibratoToken, host, time.Second, mr.WaitGroup)
		librato.Start()
	}

	// init our foremen and start it
	mr.batchForeman.Start()
	mr.handlerForeman.Start()

	// start our web server
	mr.webserver = web.NewServer(mr.CTX, mr.Config, mr.DB, mr.RP, mr.S3Client, mr.ElasticClient, mr.WaitGroup)
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
	close(mr.Quit)
	mr.Cancel()

	// stop our web server
	mr.webserver.Stop()

	mr.WaitGroup.Wait()
	mr.ElasticClient.Stop()
	logrus.Info("mailroom stopped")
	return nil
}
