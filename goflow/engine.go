package goflow

import (
	"crypto/tls"
	"net/http"
	"sync"
	"time"

	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/engine"
	"github.com/nyaruka/goflow/services/webhooks"
	"github.com/nyaruka/goflow/utils/httpx"
	"github.com/nyaruka/mailroom/config"

	"github.com/shopspring/decimal"
)

var eng, simulator flows.Engine
var engInit, simulatorInit, webhooksHTTPInit sync.Once

var webhooksHTTPClient *http.Client
var webhooksHTTPRetries *httpx.RetryConfig

var emailFactory engine.EmailServiceFactory
var classificationFactory engine.ClassificationServiceFactory
var airtimeFactory engine.AirtimeServiceFactory

// RegisterEmailServiceFactory can be used by outside callers to register a email factory
// for use by the engine
func RegisterEmailServiceFactory(factory engine.EmailServiceFactory) {
	emailFactory = factory
}

// RegisterClassificationServiceFactory can be used by outside callers to register a classification factory
// for use by the engine
func RegisterClassificationServiceFactory(factory engine.ClassificationServiceFactory) {
	classificationFactory = factory
}

// RegisterAirtimeServiceFactory can be used by outside callers to register a airtime factory
// for use by the engine
func RegisterAirtimeServiceFactory(factory engine.AirtimeServiceFactory) {
	airtimeFactory = factory
}

// Engine returns the global engine instance for use with real sessions
func Engine() flows.Engine {
	engInit.Do(func() {
		webhookHeaders := map[string]string{
			"User-Agent":      "RapidProMailroom/" + config.Mailroom.Version,
			"X-Mailroom-Mode": "normal",
		}

		httpClient, httpRetries := webhooksHTTP()

		eng = engine.NewBuilder().
			WithWebhookServiceFactory(webhooks.NewServiceFactory(httpClient, httpRetries, webhookHeaders, config.Mailroom.WebhooksMaxBodyBytes)).
			WithEmailServiceFactory(emailFactory).
			WithClassificationServiceFactory(classificationFactory).
			WithAirtimeServiceFactory(airtimeFactory).
			WithMaxStepsPerSprint(config.Mailroom.MaxStepsPerSprint).
			Build()
	})

	return eng
}

// Simulator returns the global engine instance for use with simulated sessions
func Simulator() flows.Engine {
	simulatorInit.Do(func() {
		webhookHeaders := map[string]string{
			"User-Agent":      "RapidProMailroom/" + config.Mailroom.Version,
			"X-Mailroom-Mode": "simulation",
		}

		httpClient, _ := webhooksHTTP() // don't do retries in simulator

		simulator = engine.NewBuilder().
			WithWebhookServiceFactory(webhooks.NewServiceFactory(httpClient, nil, webhookHeaders, config.Mailroom.WebhooksMaxBodyBytes)).
			WithClassificationServiceFactory(classificationFactory).   // simulated sessions do real classification
			WithEmailServiceFactory(simulatorEmailServiceFactory).     // but faked emails
			WithAirtimeServiceFactory(simulatorAirtimeServiceFactory). // and faked airtime transfers
			WithMaxStepsPerSprint(config.Mailroom.MaxStepsPerSprint).
			Build()
	})

	return simulator
}

func webhooksHTTP() (*http.Client, *httpx.RetryConfig) {
	webhooksHTTPInit.Do(func() {
		// customize the default golang transport
		t := http.DefaultTransport.(*http.Transport).Clone()
		t.MaxIdleConns = 32
		t.MaxIdleConnsPerHost = 8
		t.IdleConnTimeout = 30 * time.Second
		t.TLSClientConfig = &tls.Config{
			Renegotiation: tls.RenegotiateOnceAsClient, // support single TLS renegotiation
		}

		webhooksHTTPClient = &http.Client{
			Transport: t,
			Timeout:   time.Duration(config.Mailroom.WebhooksTimeout) * time.Millisecond,
		}

		webhooksHTTPRetries = httpx.NewExponentialRetries(
			time.Duration(config.Mailroom.WebhooksInitialBackoff)*time.Millisecond,
			config.Mailroom.WebhooksMaxRetries,
			config.Mailroom.WebhooksBackoffJitter,
		)
	})
	return webhooksHTTPClient, webhooksHTTPRetries
}

func simulatorEmailServiceFactory(session flows.Session) (flows.EmailService, error) {
	return &simulatorEmailService{}, nil
}

type simulatorEmailService struct{}

func (s *simulatorEmailService) Send(session flows.Session, addresses []string, subject, body string) error {
	return nil
}

func simulatorAirtimeServiceFactory(session flows.Session) (flows.AirtimeService, error) {
	return &simulatorAirtimeService{}, nil
}

type simulatorAirtimeService struct{}

func (s *simulatorAirtimeService) Transfer(session flows.Session, sender urns.URN, recipient urns.URN, amounts map[string]decimal.Decimal, logHTTP flows.HTTPLogCallback) (*flows.AirtimeTransfer, error) {
	transfer := &flows.AirtimeTransfer{
		Sender:        sender,
		Recipient:     recipient,
		DesiredAmount: decimal.Zero,
		ActualAmount:  decimal.Zero,
	}

	// pick arbitrary currency/amount pair in map
	for currency, amount := range amounts {
		transfer.Currency = currency
		transfer.DesiredAmount = amount
		transfer.ActualAmount = amount
		break
	}

	return transfer, nil
}
