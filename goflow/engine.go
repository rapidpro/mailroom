package goflow

import (
	"crypto/tls"
	"net/http"
	"sync"
	"time"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/engine"
	"github.com/nyaruka/goflow/flows/engine/webhooks"
	"github.com/nyaruka/mailroom/config"
)

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
			WithWebhookService(webhooks.NewService(httpClient, "RapidProMailroom/"+config.Mailroom.Version, 10000)).
			WithAirtimeService(airtimeService).
			WithMaxStepsPerSprint(config.Mailroom.MaxStepsPerSprint).
			Build()
	})

	return eng
}

var eng flows.Engine
var engInit sync.Once

var airtimeService flows.AirtimeService

func SetAirtimeService(s flows.AirtimeService) {
	airtimeService = s
}
