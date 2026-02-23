package mailroom

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/appleboy/go-fcm"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/gomodule/redigo/redis"
	"github.com/nyaruka/gocommon/aws/cwatch"
	"github.com/nyaruka/gocommon/aws/dynamo"
	"github.com/nyaruka/gocommon/aws/osearch"
	"github.com/nyaruka/mailroom/core/crons"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/web"
)

const (
	appNodesRunningKey = "app-nodes:running"
)

type Service struct {
	ctx    context.Context
	cancel context.CancelFunc

	rt        *runtime.Runtime
	workersWG *sync.WaitGroup
	quit      chan bool

	realtimeForeman  *Foreman
	batchForeman     *Foreman
	throttledForeman *Foreman

	webserver *web.Server

	// some stats are cummulative that we need to convert into increments by tracking their previous values
	dbWaitDuration time.Duration
	vkWaitDuration time.Duration
}

// NewService creates and returns a new mailroom service
func NewService(rt *runtime.Runtime) *Service {
	s := &Service{
		rt: rt,

		workersWG: &sync.WaitGroup{},
		quit:      make(chan bool),
	}
	s.ctx, s.cancel = context.WithCancel(context.Background())

	s.realtimeForeman = NewForeman(s.rt, s.rt.Queues.Realtime, rt.Config.WorkersRealtime)
	s.batchForeman = NewForeman(s.rt, s.rt.Queues.Batch, rt.Config.WorkersBatch)
	s.throttledForeman = NewForeman(s.rt, s.rt.Queues.Throttled, rt.Config.WorkersThrottled)

	return s
}

func (s *Service) Start() error {
	c := s.rt.Config

	log := slog.With("comp", "mailroom")

	// test Postgres
	if err := checkDBConnection(s.rt.DB.DB); err != nil {
		log.Error("postgres not reachable", "error", err)
	} else {
		log.Info("postgres ok")
	}
	if s.rt.ReadonlyDB != s.rt.DB.DB {
		if err := checkDBConnection(s.rt.ReadonlyDB); err != nil {
			log.Error("readonly db not reachable", "error", err)
		} else {
			log.Info("readonly db ok")
		}
	} else {
		log.Warn("no distinct readonly db configured")
	}

	// test Valkey
	vc := s.rt.VK.Get()
	defer vc.Close()
	if _, err := vc.Do("PING"); err != nil {
		log.Error("valkey not reachable", "error", err)
	} else {
		log.Info("valkey ok")
	}

	// test DynamoDB tables
	if err := dynamo.Test(s.ctx, s.rt.Dynamo.Main.Client(), c.DynamoTablePrefix+"Main", c.DynamoTablePrefix+"History"); err != nil {
		log.Error("dynamodb not reachable", "error", err)
	} else {
		log.Info("dynamodb ok")
	}

	// test S3 bucket
	if err := s.rt.S3.Test(s.ctx, c.S3AttachmentsBucket); err != nil {
		log.Error("attachments bucket not accessible", "error", err)
	} else {
		log.Info("attachments bucket ok")
	}

	// test Elasticsearch
	ping, err := s.rt.ES.Ping().Do(s.ctx)
	if err != nil {
		log.Error("elasticsearch not available", "error", err)
	} else if !ping {
		log.Error("elasticsearch cluster not reachable")
	} else {
		log.Info("elastic ok")
	}

	// test OpenSearch
	if s.rt.Search != nil {
		if err := osearch.Test(s.ctx, s.rt.Search.Messages.Client(), s.rt.Search.Messages.Index()); err != nil {
			log.Error("opensearch messages not available", "error", err)
		} else {
			log.Info("opensearch messages ok")
		}
	} else {
		log.Warn("opensearch messages not configured")
	}

	if c.AndroidCredentialsFile != "" {
		s.rt.FCM, err = fcm.NewClient(s.ctx, fcm.WithCredentialsFile(c.AndroidCredentialsFile))
		if err != nil {
			log.Error("unable to create FCM client", "error", err)
		}
	} else {
		log.Warn("fcm not configured, no android syncing")
	}

	if err := s.rt.Start(); err != nil {
		return fmt.Errorf("error starting runtime: %w", err)
	} else {
		log.Info("runtime started")
	}

	// init our foremen and start it
	s.realtimeForeman.Start(s.workersWG)
	s.batchForeman.Start(s.workersWG)
	s.throttledForeman.Start(s.workersWG)

	// start our web server
	s.webserver = web.NewServer(s.ctx, s.rt, s.workersWG)
	s.webserver.Start()

	crons.StartAll(s.rt, s.workersWG, s.quit)

	s.startMetricsReporter(time.Minute)

	if err := s.checkLastShutdown(s.ctx); err != nil {
		return err
	}

	log.Info("mailroom started", "domain", c.Domain)

	return nil
}

func (s *Service) checkLastShutdown(ctx context.Context) error {
	nodeID := fmt.Sprintf("mailroom:%s", s.rt.Config.InstanceID)
	vc := s.rt.VK.Get()
	defer vc.Close()

	exists, err := redis.Bool(redis.DoContext(vc, ctx, "HEXISTS", appNodesRunningKey, nodeID))
	if err != nil {
		return fmt.Errorf("error checking last shutdown: %w", err)
	}

	if exists {
		slog.Error("node did not shutdown cleanly last time")
	} else {
		if _, err := redis.DoContext(vc, ctx, "HSET", appNodesRunningKey, nodeID, time.Now().UTC().Format(time.RFC3339)); err != nil {
			return fmt.Errorf("error setting app node state: %w", err)
		}
	}
	return nil
}

func (s *Service) recordShutdown(ctx context.Context) error {
	nodeID := fmt.Sprintf("mailroom:%s", s.rt.Config.InstanceID)
	vc := s.rt.VK.Get()
	defer vc.Close()

	if _, err := redis.DoContext(vc, ctx, "HDEL", appNodesRunningKey, nodeID); err != nil {
		return fmt.Errorf("error recording shutdown: %w", err)
	}
	return nil
}

func (s *Service) startMetricsReporter(interval time.Duration) {
	s.workersWG.Add(1)

	report := func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		count, err := s.reportMetrics(ctx)
		cancel()
		if err != nil {
			slog.Error("error reporting metrics", "error", err)
		} else {
			slog.Info("sent metrics to cloudwatch", "count", count)
		}
	}

	go func() {
		defer func() {
			slog.Info("metrics reporter exiting")
			s.workersWG.Done()
		}()

		for {
			select {
			case <-s.quit:
				report()
				return
			case <-time.After(interval): // TODO align to half minute marks for queue sizes?
				report()
			}
		}
	}()
}

func (s *Service) reportMetrics(ctx context.Context) (int, error) {
	if s.rt.Config.MetricsReporting == "off" {
		return 0, nil
	}

	metrics := s.rt.Stats.Extract().ToMetrics(s.rt.Config.MetricsReporting == "advanced")

	realtimeSize, batchSize, throttledSize := getQueueSizes(ctx, s.rt)

	// calculate DB and valkey stats
	dbStats := s.rt.DB.Stats()
	vkStats := s.rt.VK.Stats()
	dbWaitDurationInPeriod := dbStats.WaitDuration - s.dbWaitDuration
	vkWaitDurationInPeriod := vkStats.WaitDuration - s.vkWaitDuration
	s.dbWaitDuration = dbStats.WaitDuration
	s.vkWaitDuration = vkStats.WaitDuration

	hostDim := cwatch.Dimension("Host", s.rt.Config.InstanceID)
	metrics = append(metrics,
		cwatch.Datum("DBConnectionsInUse", float64(dbStats.InUse), types.StandardUnitCount, hostDim),
		cwatch.Datum("DBConnectionWaitDuration", float64(dbWaitDurationInPeriod)/float64(time.Second), types.StandardUnitSeconds, hostDim),
		cwatch.Datum("ValkeyConnectionsInUse", float64(vkStats.ActiveCount), types.StandardUnitCount, hostDim),
		cwatch.Datum("ValkeyConnectionsWaitDuration", float64(vkWaitDurationInPeriod)/float64(time.Second), types.StandardUnitSeconds, hostDim),
		cwatch.Datum("QueuedTasks", float64(realtimeSize), types.StandardUnitCount, cwatch.Dimension("QueueName", "realtime")),
		cwatch.Datum("QueuedTasks", float64(batchSize), types.StandardUnitCount, cwatch.Dimension("QueueName", "batch")),
		cwatch.Datum("QueuedTasks", float64(throttledSize), types.StandardUnitCount, cwatch.Dimension("QueueName", "throttled")),
		cwatch.Datum("DynamoSpooledItems", float64(s.rt.Dynamo.Spool.Size()), types.StandardUnitCount, hostDim),
	)

	if err := s.rt.CW.Send(ctx, metrics...); err != nil {
		return 0, fmt.Errorf("error sending metrics: %w", err)
	}

	return len(metrics), nil
}

// Stop stops the mailroom service
func (s *Service) Stop() error {
	log := slog.With("comp", "mailroom")
	log.Info("mailroom stopping")

	s.realtimeForeman.Stop()
	s.batchForeman.Stop()
	s.throttledForeman.Stop()

	close(s.quit) // tell workers and crons to stop
	s.cancel()

	s.webserver.Stop()

	s.workersWG.Wait()

	log.Info("workers stopped")

	s.rt.Stop()

	log.Info("runtime stopped")

	if err := s.recordShutdown(context.TODO()); err != nil {
		return fmt.Errorf("error recording shutdown: %w", err)
	}

	log.Info("mailroom stopped")
	return nil
}

func checkDBConnection(db *sql.DB) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	err := db.PingContext(ctx)
	cancel()

	return err
}

func getQueueSizes(ctx context.Context, rt *runtime.Runtime) (int, int, int) {
	vc := rt.VK.Get()
	defer vc.Close()

	realtime, err := rt.Queues.Realtime.Size(ctx, vc)
	if err != nil {
		slog.Error("error calculating realtime queue size", "error", err)
	}
	batch, err := rt.Queues.Batch.Size(ctx, vc)
	if err != nil {
		slog.Error("error calculating batch queue size", "error", err)
	}
	throttled, err := rt.Queues.Throttled.Size(ctx, vc)
	if err != nil {
		slog.Error("error calculating throttled queue size", "error", err)
	}

	return realtime, batch, throttled
}
