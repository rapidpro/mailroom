package main

import (
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/nyaruka/ezconf"
	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/logrus_sentry"
	"github.com/nyaruka/mailroom"
	"github.com/nyaruka/mailroom/config"

	_ "github.com/nyaruka/mailroom/core/handlers"
	_ "github.com/nyaruka/mailroom/core/hooks"
	_ "github.com/nyaruka/mailroom/core/ivr/nexmo"
	_ "github.com/nyaruka/mailroom/core/ivr/twiml"
	_ "github.com/nyaruka/mailroom/core/tasks/broadcasts"
	_ "github.com/nyaruka/mailroom/core/tasks/campaigns"
	_ "github.com/nyaruka/mailroom/core/tasks/contacts"
	_ "github.com/nyaruka/mailroom/core/tasks/expirations"
	_ "github.com/nyaruka/mailroom/core/tasks/groups"
	_ "github.com/nyaruka/mailroom/core/tasks/interrupts"
	_ "github.com/nyaruka/mailroom/core/tasks/ivr"
	_ "github.com/nyaruka/mailroom/core/tasks/schedules"
	_ "github.com/nyaruka/mailroom/core/tasks/starts"
	_ "github.com/nyaruka/mailroom/core/tasks/stats"
	_ "github.com/nyaruka/mailroom/core/tasks/timeouts"
	_ "github.com/nyaruka/mailroom/services/tickets/mailgun"
	_ "github.com/nyaruka/mailroom/services/tickets/rocketchat"
	_ "github.com/nyaruka/mailroom/services/tickets/zendesk"
	_ "github.com/nyaruka/mailroom/web/contact"
	_ "github.com/nyaruka/mailroom/web/docs"
	_ "github.com/nyaruka/mailroom/web/expression"
	_ "github.com/nyaruka/mailroom/web/flow"
	_ "github.com/nyaruka/mailroom/web/ivr"
	_ "github.com/nyaruka/mailroom/web/org"
	_ "github.com/nyaruka/mailroom/web/po"
	_ "github.com/nyaruka/mailroom/web/simulation"
	_ "github.com/nyaruka/mailroom/web/surveyor"
	_ "github.com/nyaruka/mailroom/web/ticket"

	_ "github.com/lib/pq"
	"github.com/sirupsen/logrus"
)

var version = "Dev"

func main() {
	config := config.Mailroom
	loader := ezconf.NewLoader(
		config,
		"mailroom", "Mailroom - flow event handler for RapidPro",
		[]string{"mailroom.toml"},
	)
	loader.MustLoad()

	// ensure config is valid
	if err := config.Validate(); err != nil {
		logrus.Fatalf("invalid config: %s", err)
	}

	// if we have a custom version, use it
	if version != "Dev" {
		config.Version = version
	}

	// configure our logger
	logrus.SetOutput(os.Stdout)
	level, err := logrus.ParseLevel(config.LogLevel)
	if err != nil {
		logrus.Fatalf("invalid log level '%s'", level)
	}
	logrus.SetLevel(level)

	// if we have a DSN entry, try to initialize it
	if config.SentryDSN != "" {
		hook, err := logrus_sentry.NewSentryHook(config.SentryDSN, []logrus.Level{logrus.PanicLevel, logrus.FatalLevel, logrus.ErrorLevel})
		hook.Timeout = 0
		hook.StacktraceConfiguration.Enable = true
		hook.StacktraceConfiguration.Skip = 4
		hook.StacktraceConfiguration.Context = 5
		hook.StacktraceConfiguration.IncludeErrorBreadcrumb = true
		if err != nil {
			logrus.Fatalf("invalid sentry DSN: '%s': %s", config.SentryDSN, err)
		}
		logrus.StandardLogger().Hooks.Add(hook)
	}

	if config.UUIDSeed != 0 {
		uuids.SetGenerator(uuids.NewSeededGenerator(int64(config.UUIDSeed)))
		logrus.WithField("uuid-seed", config.UUIDSeed).Warn("using seeded UUID generation which is only appropriate for testing environments")
	}

	mr := mailroom.NewMailroom(config)
	err = mr.Start()
	if err != nil {
		logrus.Fatalf("error starting server: %s", err)
	}

	// handle our signals
	handleSignals(mr)
}

// handleSignals takes care of trapping quit, interrupt or terminate signals and doing the right thing
func handleSignals(mr *mailroom.Mailroom) {
	sigs := make(chan os.Signal)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	for {
		sig := <-sigs
		switch sig {
		case syscall.SIGQUIT:
			buf := make([]byte, 1<<20)
			stacklen := runtime.Stack(buf, true)
			logrus.WithField("comp", "main").WithField("signal", sig).Info("received quit signal, dumping stack")
			logrus.Printf("\n%s", buf[:stacklen])
		case syscall.SIGINT, syscall.SIGTERM:
			logrus.WithField("comp", "main").WithField("signal", sig).Info("received exit signal, exiting")
			mr.Stop()
			return
		}
	}
}
