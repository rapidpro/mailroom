package goflow

import (
	"crypto/tls"
	"net/http"
	"sync"
	"time"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/engine"
	"github.com/nyaruka/mailroom/config"
)

var eng flows.Engine
var engInit sync.Once

var serviceResolver ServiceResolver

// Engine returns the global engine instance for use in mailroom
func Engine() flows.Engine {
	engInit.Do(func() {
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

		eng = engine.NewBuilder().
			WithHTTPClient(httpClient).
			WithWebhookService(serviceResolver.Webhooks).
			WithAirtimeService(serviceResolver.Airtime).
			WithMaxStepsPerSprint(config.Mailroom.MaxStepsPerSprint).
			Build()
	})

	return eng
}

type ServiceResolver interface {
	Webhooks(flows.Session) flows.WebhookProvider
	Airtime(flows.Session) flows.AirtimeProvider
}

func SetServiceResolver(resolver ServiceResolver) {
	serviceResolver = resolver
}
