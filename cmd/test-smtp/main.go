package main

import (
	"net/url"
	"strconv"

	"github.com/nyaruka/ezconf"
	"github.com/sirupsen/logrus"
	"gopkg.in/mail.v2"
)

type Config struct {
	URL     string `help:"the SMTP formatted URL to use to test sending"`
	To      string `help:"the email address to send to"`
	Subject string `help:"the email subject to send"`
	Body    string `help:"the email body to send"`
}

func main() {
	// get our smtp server config
	options := &Config{
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

	// parse it
	url, err := url.Parse(options.URL)
	if err != nil {
		logrus.WithError(err).Fatalf("unable to parse smtp config: %s", options.URL)
	}

	// figure out our port
	sPort := url.Port()
	if sPort == "" {
		sPort = "25"
	}
	port, err := strconv.Atoi(sPort)
	if err != nil {
		logrus.WithError(err).Fatalf("invalid port configuration: %s", options.URL)
	}

	// and our user and password
	if url.User == nil {
		logrus.Fatalf("no user set for smtp server: %s", options.URL)
	}
	password, _ := url.User.Password()

	// get our from
	from := url.Query()["from"]
	if len(from) == 0 {
		from = []string{url.User.Username()}
	}

	// create our dialer for our org
	d := mail.NewDialer(url.Hostname(), port, url.User.Username(), password)

	// send each of our emails, errors are logged but don't stop us from trying to send our other emails
	m := mail.NewMessage()
	m.SetHeader("From", from[0])
	m.SetHeader("To", options.To)
	m.SetHeader("Subject", options.Subject)
	m.SetBody("text/plain", options.Body)

	logrus.WithFields(logrus.Fields{
		"hostname": url.Hostname(),
		"port":     port,
		"username": url.User.Username(),
		"password": password,
	}).Info("attempting to send email")

	err = d.DialAndSend(m)
	if err != nil {
		logrus.WithError(err).Fatal("error sending email")
	}

	logrus.Info("email sent")
}
