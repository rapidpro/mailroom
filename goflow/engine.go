package goflow

import (
	"crypto/tls"
	"net/http"
	"sync"
	"time"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/engine"
	"github.com/nyaruka/goflow/services/webhooks"
	"github.com/nyaruka/mailroom/config"
)

var eng flows.Engine
var engInit sync.Once
var classificationFactory engine.ClassificationServiceFactory

// RegisterClassificationServiceFactory can be used by outside callers to register a classification factory
// for use by the engine
func RegisterClassificationServiceFactory(factory engine.ClassificationServiceFactory) {
	classificationFactory = factory
}

// Engine returns the global engine instance for use in mailroom
func Engine() flows.Engine {
	httpClient := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:    10,
			IdleConnTimeout: 30 * time.Second,
			TLSClientConfig: &tls.Config{
				Renegotiation: tls.RenegotiateOnceAsClient, // support single TLS renegotiation
			},
		},
		Timeout: time.Duration(15 * time.Second),
	}

	engInit.Do(func() {
		eng = engine.NewBuilder().
			WithHTTPClient(httpClient).
			WithWebhookServiceFactory(func(flows.Session) (flows.WebhookService, error) {
				return webhooks.NewService("RapidProMailroom/"+config.Mailroom.Version, 10000), nil
			}).
			WithClassificationServiceFactory(classificationFactory).
			WithMaxStepsPerSprint(config.Mailroom.MaxStepsPerSprint).
			Build()
	})

	return eng
}
