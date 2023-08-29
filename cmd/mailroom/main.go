package main

import (
	ulog "log"
	"log/slog"
	"os"
	"os/signal"
	goruntime "runtime"
	"syscall"

	_ "github.com/lib/pq"
	"github.com/nyaruka/ezconf"
	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/logrus_sentry"
	"github.com/nyaruka/mailroom"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/utils"
	"github.com/sirupsen/logrus"

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
	_ "github.com/nyaruka/mailroom/services/tickets/intern"
	_ "github.com/nyaruka/mailroom/services/tickets/mailgun"
	_ "github.com/nyaruka/mailroom/services/tickets/rocketchat"
	_ "github.com/nyaruka/mailroom/services/tickets/zendesk"
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

	level, err := logrus.ParseLevel(config.LogLevel)
	if err != nil {
		slog.Error("invalid log level", "level", level)
		os.Exit(1)
	}

	logrus.SetLevel(level)
	logrus.SetOutput(os.Stdout)
	logrus.SetFormatter(&logrus.TextFormatter{})

	// configure golang std structured logging to route to logrus
	slog.SetDefault(slog.New(utils.NewLogrusHandler(logrus.StandardLogger())))

	log := slog.With("comp", "main")
	log.Info("starting mailroom", "version", version, "released", date)

	// if we have a DSN entry, try to initialize it
	if config.SentryDSN != "" {
		hook, err := logrus_sentry.NewSentryHook(config.SentryDSN, []logrus.Level{logrus.PanicLevel, logrus.FatalLevel, logrus.ErrorLevel})
		hook.Timeout = 0
		hook.StacktraceConfiguration.Enable = true
		hook.StacktraceConfiguration.Skip = 4
		hook.StacktraceConfiguration.Context = 5
		hook.StacktraceConfiguration.IncludeErrorBreadcrumb = true
		if err != nil {
			log.Error("unable to configure sentry hook", "dsn", config.SentryDSN, "error", err)
			os.Exit(1)
		}
		logrus.StandardLogger().Hooks.Add(hook)
	}

	if config.UUIDSeed != 0 {
		uuids.SetGenerator(uuids.NewSeededGenerator(int64(config.UUIDSeed)))
		log.Warn("using seeded UUID generation", "uuid-seed", config.UUIDSeed)
	}

	mr := mailroom.NewMailroom(config)
	err = mr.Start()
	if err != nil {
		log.Error("unable to start server", "error", err)
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
