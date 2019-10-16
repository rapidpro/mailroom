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
	// customize the default golang transport
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConns = 32
	t.MaxIdleConnsPerHost = 8
	t.IdleConnTimeout = 30 * time.Second
	t.TLSClientConfig = &tls.Config{
		Renegotiation: tls.RenegotiateOnceAsClient, // support single TLS renegotiation
	}

	httpClient := &http.Client{
		Transport: t,
		Timeout:   time.Duration(15 * time.Second),
	}

	engInit.Do(func() {
		eng = engine.NewBuilder().
			WithHTTPClient(httpClient).
			WithWebhookServiceFactory(webhooks.NewServiceFactory("RapidProMailroom/"+config.Mailroom.Version, 10000)).
			WithClassificationServiceFactory(classificationFactory).
			WithMaxStepsPerSprint(config.Mailroom.MaxStepsPerSprint).
			Build()
	})

	return eng
}
