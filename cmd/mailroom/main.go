package main

import (
	ulog "log"
	"log/slog"
	"os"
	"os/signal"
	goruntime "runtime"
	"syscall"
	"time"

	"github.com/getsentry/sentry-go"
	_ "github.com/lib/pq"
	"github.com/nyaruka/ezconf"
	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/mailroom"
	"github.com/nyaruka/mailroom/runtime"
	slogmulti "github.com/samber/slog-multi"
	slogsentry "github.com/samber/slog-sentry"

	_ "github.com/nyaruka/mailroom/core/handlers"
	_ "github.com/nyaruka/mailroom/core/hooks"
	_ "github.com/nyaruka/mailroom/core/tasks/analytics"
	_ "github.com/nyaruka/mailroom/core/tasks/campaigns"
	_ "github.com/nyaruka/mailroom/core/tasks/contacts"
	_ "github.com/nyaruka/mailroom/core/tasks/expirations"
	_ "github.com/nyaruka/mailroom/core/tasks/handler"
	_ "github.com/nyaruka/mailroom/core/tasks/incidents"
	_ "github.com/nyaruka/mailroom/core/tasks/interrupts"
	_ "github.com/nyaruka/mailroom/core/tasks/ivr"
	_ "github.com/nyaruka/mailroom/core/tasks/msgs"
	_ "github.com/nyaruka/mailroom/core/tasks/schedules"
	_ "github.com/nyaruka/mailroom/core/tasks/starts"
	_ "github.com/nyaruka/mailroom/core/tasks/timeouts"
	_ "github.com/nyaruka/mailroom/services/ivr/twiml"
	_ "github.com/nyaruka/mailroom/services/ivr/vonage"
	_ "github.com/nyaruka/mailroom/web/contact"
	_ "github.com/nyaruka/mailroom/web/docs"
	_ "github.com/nyaruka/mailroom/web/flow"
	_ "github.com/nyaruka/mailroom/web/ivr"
	_ "github.com/nyaruka/mailroom/web/msg"
	_ "github.com/nyaruka/mailroom/web/org"
	_ "github.com/nyaruka/mailroom/web/po"
	_ "github.com/nyaruka/mailroom/web/simulation"
	_ "github.com/nyaruka/mailroom/web/surveyor"
	_ "github.com/nyaruka/mailroom/web/ticket"
)

var (
	// https://goreleaser.com/cookbooks/using-main.version
	version = "dev"
	date    = "unknown"
)

func main() {
	config := runtime.NewDefaultConfig()
	config.Version = version
	loader := ezconf.NewLoader(
		config,
		"mailroom", "Mailroom - flow event handler for RapidPro",
		[]string{"mailroom.toml"},
	)
	loader.MustLoad()

	// ensure config is valid
	if err := config.Validate(); err != nil {
		slog.Error("invalid config", "error", err)
		os.Exit(1)
	}

	var level slog.Level
	err := level.UnmarshalText([]byte(config.LogLevel))
	if err != nil {
		ulog.Fatalf("invalid log level %s", level)
		os.Exit(1)
	}

	// configure our logger
	logHandler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	slog.SetDefault(slog.New(logHandler))

	logger := slog.With("comp", "main")
	logger.Info("starting mailroom", "version", version, "released", date)

	// if we have a DSN entry, try to initialize it
	if config.SentryDSN != "" {
		err := sentry.Init(sentry.ClientOptions{
			Dsn:           config.SentryDSN,
			EnableTracing: false,
		})
		if err != nil {
			ulog.Fatalf("error initiating sentry client, error %s, dsn %s", err, config.SentryDSN)
			os.Exit(1)
		}

		defer sentry.Flush(2 * time.Second)

		logger = slog.New(
			slogmulti.Fanout(
				logHandler,
				slogsentry.Option{Level: slog.LevelError}.NewSentryHandler(),
			),
		)
		logger = logger.With("release", version)
		slog.SetDefault(logger)
	}

	if config.UUIDSeed != 0 {
		uuids.SetGenerator(uuids.NewSeededGenerator(int64(config.UUIDSeed)))
		logger.Warn("using seeded UUID generation", "uuid-seed", config.UUIDSeed)
	}

	mr := mailroom.NewMailroom(config)
	err = mr.Start()
	if err != nil {
		logger.Error("unable to start server", "error", err)
	}

	// handle our signals
	handleSignals(mr)
}

// handleSignals takes care of trapping quit, interrupt or terminate signals and doing the right thing
func handleSignals(mr *mailroom.Mailroom) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	for {
		sig := <-sigs
		log := slog.With("comp", "main", "signal", sig)

		switch sig {
		case syscall.SIGQUIT:
			buf := make([]byte, 1<<20)
			stacklen := goruntime.Stack(buf, true)
			log.Info("received quit signal, dumping stack")
			ulog.Printf("\n%s", buf[:stacklen])
		case syscall.SIGINT, syscall.SIGTERM:
			log.Info("received exit signal, exiting")
			mr.Stop()
			return
		}
	}
}
