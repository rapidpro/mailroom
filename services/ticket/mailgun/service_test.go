package mailgun_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/assets/static/types"
	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/test"
	"github.com/nyaruka/goflow/utils/dates"
	"github.com/nyaruka/goflow/utils/httpx"
	"github.com/nyaruka/goflow/utils/uuids"
	"github.com/nyaruka/mailroom/services/ticket/mailgun"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestService(t *testing.T) {
	session, _, err := test.CreateTestSession("", envs.RedactionPolicyNone)
	require.NoError(t, err)

	defer uuids.SetGenerator(uuids.DefaultGenerator)
	defer dates.SetNowSource(dates.DefaultNowSource)
	defer httpx.SetRequestor(httpx.DefaultRequestor)

	uuids.SetGenerator(uuids.NewSeededGenerator(12345))
	dates.SetNowSource(dates.NewSequentialNowSource(time.Date(2019, 10, 7, 15, 21, 30, 123456789, time.UTC)))
	httpx.SetRequestor(httpx.NewMockRequestor(map[string][]httpx.MockResponse{
		"https://api.mailgun.net/v3/tickets.rapidpro.io/messages": {
			httpx.MockConnectionError,
			httpx.NewMockResponse(200, nil, `{
				"id": "<20200426161758.1.590432020254B2BF@tickets.rapidpro.io>",
				"message": "Queued. Thank you."
			}`),
		},
	}))

	ticketer := flows.NewTicketer(types.NewTicketer(assets.TicketerUUID(uuids.New()), "Support", "mailgun"))

	_, err = mailgun.NewService(
		http.DefaultClient,
		nil,
		ticketer,
		map[string]string{},
	)
	assert.EqualError(t, err, "missing domain or api_key or to_address or url_base in mailgun config")

	svc, err := mailgun.NewService(
		http.DefaultClient,
		nil,
		ticketer,
		map[string]string{
			"domain":     "tickets.rapidpro.io",
			"api_key":    "123456789",
			"to_address": "bob@acme.com",
			"url_base":   "http://app.rapidpro.io",
		},
	)
	require.NoError(t, err)

	httpLogger := &flows.HTTPLogger{}

	_, err = svc.Open(session, "Need help", "Where are my cookies?", httpLogger.Log)
	assert.EqualError(t, err, "error calling mailgun API: unable to connect to server")

	httpLogger = &flows.HTTPLogger{}

	ticket, err := svc.Open(session, "Need help", "Where are my cookies?", httpLogger.Log)
	assert.NoError(t, err)
	assert.Equal(t, &flows.Ticket{
		UUID:       flows.TicketUUID("9688d21d-95aa-4bed-afc7-f31b35731a3d"),
		Ticketer:   ticketer.Reference(),
		Subject:    "Need help",
		Body:       "Where are my cookies?",
		ExternalID: "<20200426161758.1.590432020254B2BF@tickets.rapidpro.io>",
	}, ticket)

	assert.Equal(t, 1, len(httpLogger.Logs))
	assert.Equal(t, "https://api.mailgun.net/v3/tickets.rapidpro.io/messages", httpLogger.Logs[0].URL)
	assert.Contains(t, httpLogger.Logs[0].Request, "****************") // check token redacted
	assert.NotContains(t, httpLogger.Logs[0].Request, "YXBpOjEyMzQ1Njc4OQ==")
	assert.NotContains(t, httpLogger.Logs[0].Request, "sesame")
}
