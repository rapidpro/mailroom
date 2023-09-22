package main

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/nyaruka/ezconf"
	"github.com/nyaruka/goflow/utils/smtpx"
)

type config struct {
	URL     string `help:"the SMTP formatted URL to use to test sending"`
	To      string `help:"the email address to send to"`
	Subject string `help:"the email subject to send"`
	Body    string `help:"the email body to send"`
}

func main() {
	// get our smtp server config
	options := &config{
		URL:     "smtp://foo%40zap.com:opensesame@smtp.gmail.com:587/?from=foo%40zap.com&tls=true",
		To:      "test@temba.io",
		Subject: "Test Email",
		Body:    "Test Body",
	}
	loader := ezconf.NewLoader(
		options,
		"test-smtp", "SMTP Tester - test SMTP settings by sending an email",
		nil,
	)
	loader.MustLoad()

	client, err := smtpx.NewClientFromURL(options.URL)
	if err != nil {
		slog.Error(fmt.Sprintf("unable to parse smtp config: %s", options.URL), "error", err)
		os.Exit(1)
	}

	m := smtpx.NewMessage([]string{options.To}, options.Subject, options.Body, "")

	err = smtpx.Send(client, m, nil)
	if err != nil {
		slog.Error("error sending email", "error", err)
	}

	slog.Info("email sent")
}
