package runtime

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"firebase.google.com/go/v4/messaging"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/gomodule/redigo/redis"
	"github.com/nyaruka/gocommon/aws/cwatch"
	"github.com/nyaruka/gocommon/aws/s3x"
	"github.com/nyaruka/vkutil"
	"github.com/vinovest/sqlx"
)

// Runtime represents the set of services required to run many Mailroom functions. Used as a wrapper for
// those services to simplify call signatures but not create a direct dependency to Mailroom or Server
type Runtime struct {
	Config *Config

	DB         *sqlx.DB
	ReadonlyDB *sql.DB
	VK         *redis.Pool
	S3         *s3x.Service
	ES         *elasticsearch.TypedClient
	Dynamo     *Dynamo
	OS         *OpenSearch
	CW         *cwatch.Service
	FCM        FCMClient

	Queues *Queues
	Stats  *StatsCollector
}

// FCMClient is an interface to allow mocking in tests
type FCMClient interface {
	Send(ctx context.Context, message ...*messaging.Message) (*messaging.BatchResponse, error)
}

func NewRuntime(cfg *Config) (*Runtime, error) {
	rt := &Runtime{Config: cfg}

	var err error

	rt.DB, err = createPostgresPool(cfg.DB, cfg.DBPoolSize)
	if err != nil {
		return nil, fmt.Errorf("error creating Postgres connection pool: %w", err)
	}

	if cfg.ReadonlyDB != "" {
		roDB, err := createPostgresPool(cfg.ReadonlyDB, cfg.DBPoolSize)
		if err != nil {
			return nil, fmt.Errorf("error creating readonly Postgres connection pool: %w", err)
		}

		rt.ReadonlyDB = roDB.DB
	} else {
		rt.ReadonlyDB = rt.DB.DB
	}

	rt.Dynamo, err = newDynamo(cfg)
	if err != nil {
		return nil, err
	}

	rt.VK, err = vkutil.NewPool(cfg.Valkey)
	if err != nil {
		return nil, fmt.Errorf("error creating Valkey pool: %w", err)
	}

	rt.S3, err = s3x.NewService(cfg.AWSAccessKeyID, cfg.AWSSecretAccessKey, cfg.AWSRegion, cfg.S3Endpoint, cfg.S3PathStyle)
	if err != nil {
		return nil, fmt.Errorf("error creating S3 service: %w", err)
	}

	rt.ES, err = elasticsearch.NewTypedClient(elasticsearch.Config{Addresses: []string{cfg.Elastic}, Username: cfg.ElasticUsername, Password: cfg.ElasticPassword})
	if err != nil {
		return nil, fmt.Errorf("error creating Elasticsearch client: %w", err)
	}

	if cfg.OSEndpoint != "" {
		rt.OS, err = newOpenSearch(cfg)
		if err != nil {
			return nil, err
		}
	}

	rt.CW, err = cwatch.NewService(cfg.AWSAccessKeyID, cfg.AWSSecretAccessKey, cfg.AWSRegion, cfg.CloudwatchNamespace, cfg.DeploymentID)
	if err != nil {
		return nil, fmt.Errorf("error creating Cloudwatch service: %w", err)
	}

	rt.Queues = newQueues(cfg)
	rt.Stats = NewStatsCollector(rt.VK)

	return rt, nil
}

func (r *Runtime) Start() error {
	if err := r.Dynamo.start(); err != nil {
		return err
	}
	if err := r.OS.start(); err != nil {
		return err
	}
	return nil
}

func (r *Runtime) Stop() {
	r.Dynamo.stop()
	r.OS.stop()
}

func createPostgresPool(url string, maxOpenConns int) (*sqlx.DB, error) {
	db, err := sqlx.Open("postgres", url)
	if err != nil {
		return nil, fmt.Errorf("unable to open database connection: '%s': %w", url, err)
	}

	db.SetMaxIdleConns(8)
	db.SetMaxOpenConns(maxOpenConns)
	db.SetConnMaxLifetime(time.Minute * 30)

	return db, nil
}
