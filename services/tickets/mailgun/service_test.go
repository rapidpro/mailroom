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
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/services/tickets/mailgun"
	"github.com/nyaruka/mailroom/testsuite"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpenAndForward(t *testing.T) {
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
			"brand_name": "ACME",
			"url_base":   "http://app.rapidpro.io",
		},
	)
	require.NoError(t, err)

	logger := &flows.HTTPLogger{}

	_, err = svc.Open(session, "Need help", "Where are my cookies?", logger.Log)
	assert.EqualError(t, err, "error calling mailgun API: unable to connect to server")

	logger = &flows.HTTPLogger{}

	ticket, err := svc.Open(session, "Need help", "Where are my cookies?", logger.Log)
	assert.NoError(t, err)
	assert.Equal(t, &flows.Ticket{
		UUID:       flows.TicketUUID("9688d21d-95aa-4bed-afc7-f31b35731a3d"),
		Ticketer:   ticketer.Reference(),
		Subject:    "Need help",
		Body:       "Where are my cookies?",
		ExternalID: "<20200426161758.1.590432020254B2BF@tickets.rapidpro.io>",
	}, ticket)

	assert.Equal(t, 1, len(logger.Logs))
	testsuite.AssertSnapshot(t, "open_ticket.dump", logger.Logs[0].Request)

	dbTicket := models.NewTicket(ticket.UUID, models.Org1, models.CathyID, models.MailgunID, "", "Need help", "Where are my cookies?", map[string]interface{}{
		"contact-uuid":    string(models.CathyUUID),
		"contact-display": "Cathy",
	})

	logger = &flows.HTTPLogger{}
	err = svc.Forward(dbTicket, flows.MsgUUID("ca5607f0-cba8-4c94-9cd5-c4fbc24aa767"), "It's urgent", logger.Log)

	assert.NoError(t, err)
	assert.Equal(t, 1, len(logger.Logs))
	testsuite.AssertSnapshot(t, "forward_message.dump", logger.Logs[0].Request)
}

func TestCloseAndReopen(t *testing.T) {
	defer uuids.SetGenerator(uuids.DefaultGenerator)
	defer httpx.SetRequestor(httpx.DefaultRequestor)

	uuids.SetGenerator(uuids.NewSeededGenerator(12345))
	httpx.SetRequestor(httpx.NewMockRequestor(map[string][]httpx.MockResponse{
		"https://api.mailgun.net/v3/tickets.rapidpro.io/messages": {
			httpx.NewMockResponse(200, nil, `{
				"id": "<20200426161758.1.590432020254B2BF@tickets.rapidpro.io>",
				"message": "Queued. Thank you."
			}`),
			httpx.NewMockResponse(200, nil, `{
				"id": "<20200426161758.1.590432020254B2BF@tickets.rapidpro.io>",
				"message": "Queued. Thank you."
			}`),
			httpx.NewMockResponse(200, nil, `{
				"id": "<20200426161758.1.590432020254B2BF@tickets.rapidpro.io>",
				"message": "Queued. Thank you."
			}`),
		},
	}))

	ticketer := flows.NewTicketer(types.NewTicketer(assets.TicketerUUID(uuids.New()), "Support", "mailgun"))
	svc, err := mailgun.NewService(
		http.DefaultClient,
		nil,
		ticketer,
		map[string]string{
			"domain":     "tickets.rapidpro.io",
			"api_key":    "123456789",
			"to_address": "bob@acme.com",
			"brand_name": "ACME",
			"url_base":   "http://app.rapidpro.io",
		},
	)
	require.NoError(t, err)

	logger := &flows.HTTPLogger{}
	ticket1 := models.NewTicket("88bfa1dc-be33-45c2-b469-294ecb0eba90", models.Org1, models.CathyID, models.ZendeskID, "12", "New ticket", "Where my cookies?", nil)
	ticket2 := models.NewTicket("645eee60-7e84-4a9e-ade3-4fce01ae28f1", models.Org1, models.BobID, models.ZendeskID, "14", "Second ticket", "Where my shoes?", nil)

	err = svc.Close([]*models.Ticket{ticket1, ticket2}, logger.Log)

	assert.NoError(t, err)
	testsuite.AssertSnapshot(t, "close_tickets.dump", logger.Logs[0].Request)

	err = svc.Reopen([]*models.Ticket{ticket2}, logger.Log)

	assert.NoError(t, err)
	testsuite.AssertSnapshot(t, "reopen_tickets.dump", logger.Logs[1].Request)
}
