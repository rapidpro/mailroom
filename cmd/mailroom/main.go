package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/evalphobia/logrus_sentry"
	_ "github.com/lib/pq"
	"github.com/nyaruka/ezconf"
	"github.com/nyaruka/mailroom"
	"github.com/sirupsen/logrus"

	_ "github.com/nyaruka/mailroom/campaigns"
	_ "github.com/nyaruka/mailroom/expirations"
	_ "github.com/nyaruka/mailroom/handlers"
	_ "github.com/nyaruka/mailroom/starts"
)

var version = "Dev"

func main() {
	config := mailroom.NewMailroomConfig()
	loader := ezconf.NewLoader(
		config,
		"mailroom", "Mailroom - flow event handler for RapidPro",
		[]string{"mailroom.toml"},
	)
	loader.MustLoad()

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
		if err != nil {
			logrus.Fatalf("invalid sentry DSN: '%s': %s", config.SentryDSN, err)
		}
		logrus.StandardLogger().Hooks.Add(hook)
	}

	mr := mailroom.NewMailroom(config)
	err = mr.Start()
	if err != nil {
		logrus.Fatalf("error starting server: %s", err)
	}

	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	logrus.WithField("comp", "main").WithField("signal", <-ch).Info("stopping")

	mr.Stop()
}
