package goflow

import (
	"crypto/tls"
	"net/http"
	"sync"
	"time"

	"github.com/nyaruka/goflow/utils/httpx"
	"github.com/nyaruka/mailroom/config"
)

var httpInit sync.Once

var httpClient *http.Client
var httpRetries *httpx.RetryConfig
var httpAccess *httpx.AccessConfig

// HTTP returns the configuration objects for HTTP calls from the engine and its services
func HTTP() (*http.Client, *httpx.RetryConfig, *httpx.AccessConfig) {
	httpInit.Do(func() {
		// customize the default golang transport
		t := http.DefaultTransport.(*http.Transport).Clone()
		t.MaxIdleConns = 32
		t.MaxIdleConnsPerHost = 8
		t.IdleConnTimeout = 30 * time.Second
		t.TLSClientConfig = &tls.Config{
			Renegotiation: tls.RenegotiateOnceAsClient, // support single TLS renegotiation
		}

		httpClient = &http.Client{
			Transport: t,
			Timeout:   time.Duration(config.Mailroom.WebhooksTimeout) * time.Millisecond,
		}

		httpRetries = httpx.NewExponentialRetries(
			time.Duration(config.Mailroom.WebhooksInitialBackoff)*time.Millisecond,
			config.Mailroom.WebhooksMaxRetries,
			config.Mailroom.WebhooksBackoffJitter,
		)

		disallowedIPs, _ := config.Mailroom.ParseDisallowedIPs()
		httpAccess = httpx.NewAccessConfig(10*time.Second, disallowedIPs)
	})
	return httpClient, httpRetries, httpAccess
}
