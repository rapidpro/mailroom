package goflow

import (
	"sync"

	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/engine"
	"github.com/nyaruka/goflow/services/webhooks"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/shopspring/decimal"
)

var eng, simulator flows.Engine
var engInit, simulatorInit sync.Once

var emailFactory func(*runtime.Config) engine.EmailServiceFactory
var classificationFactory func(*runtime.Config) engine.ClassificationServiceFactory
var airtimeFactory func(*runtime.Config) engine.AirtimeServiceFactory

// RegisterEmailServiceFactory can be used by outside callers to register a email factory
// for use by the engine
func RegisterEmailServiceFactory(f func(*runtime.Config) engine.EmailServiceFactory) {
	emailFactory = f
}

// RegisterClassificationServiceFactory can be used by outside callers to register a classification factory
// for use by the engine
func RegisterClassificationServiceFactory(f func(*runtime.Config) engine.ClassificationServiceFactory) {
	classificationFactory = f
}

// RegisterAirtimeServiceFactory can be used by outside callers to register a airtime factory
// for use by the engine
func RegisterAirtimeServiceFactory(f func(*runtime.Config) engine.AirtimeServiceFactory) {
	airtimeFactory = f
}

// Engine returns the global engine instance for use with real sessions
func Engine(c *runtime.Config) flows.Engine {
	engInit.Do(func() {
		webhookHeaders := map[string]string{
			"User-Agent":      "RapidProMailroom/" + c.Version,
			"X-Mailroom-Mode": "normal",
		}

		httpClient, httpRetries, httpAccess := HTTP(c)

		eng = engine.NewBuilder().
			WithWebhookServiceFactory(webhooks.NewServiceFactory(httpClient, httpRetries, httpAccess, webhookHeaders, c.WebhooksMaxBodyBytes)).
			WithClassificationServiceFactory(classificationFactory(c)).
			WithEmailServiceFactory(emailFactory(c)).
			WithAirtimeServiceFactory(airtimeFactory(c)).
			WithMaxStepsPerSprint(c.MaxStepsPerSprint).
			WithMaxResumesPerSession(c.MaxResumesPerSession).
			WithMaxFieldChars(c.MaxValueLength).
			WithMaxResultChars(c.MaxValueLength).
			Build()
	})

	return eng
}

// Simulator returns the global engine instance for use with simulated sessions
func Simulator(c *runtime.Config) flows.Engine {
	simulatorInit.Do(func() {
		webhookHeaders := map[string]string{
			"User-Agent":      "RapidProMailroom/" + c.Version,
			"X-Mailroom-Mode": "simulation",
		}

		httpClient, _, httpAccess := HTTP(c) // don't do retries in simulator

		simulator = engine.NewBuilder().
			WithWebhookServiceFactory(webhooks.NewServiceFactory(httpClient, nil, httpAccess, webhookHeaders, c.WebhooksMaxBodyBytes)).
			WithClassificationServiceFactory(classificationFactory(c)). // simulated sessions do real classification
			WithEmailServiceFactory(simulatorEmailServiceFactory).      // but faked emails
			WithAirtimeServiceFactory(simulatorAirtimeServiceFactory).  // and faked airtime transfers
			WithMaxStepsPerSprint(c.MaxStepsPerSprint).
			WithMaxResumesPerSession(c.MaxResumesPerSession).
			WithMaxFieldChars(c.MaxValueLength).
			WithMaxResultChars(c.MaxValueLength).
			Build()
	})

	return simulator
}

func simulatorEmailServiceFactory(flows.SessionAssets) (flows.EmailService, error) {
	return &simulatorEmailService{}, nil
}

type simulatorEmailService struct{}

func (s *simulatorEmailService) Send(addresses []string, subject, body string) error {
	return nil
}

func simulatorAirtimeServiceFactory(flows.SessionAssets) (flows.AirtimeService, error) {
	return &simulatorAirtimeService{}, nil
}

type simulatorAirtimeService struct{}

func (s *simulatorAirtimeService) Transfer(sender urns.URN, recipient urns.URN, amounts map[string]decimal.Decimal, logHTTP flows.HTTPLogCallback) (*flows.AirtimeTransfer, error) {
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
