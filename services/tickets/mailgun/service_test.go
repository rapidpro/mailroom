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
	assert.Equal(t, "POST /v3/tickets.rapidpro.io/messages HTTP/1.1\r\nHost: api.mailgun.net\r\nUser-Agent: Go-http-client/1.1\r\nContent-Length: 768\r\nAuthorization: Basic ****************\r\nContent-Type: multipart/form-data; boundary=297611a6-b583-45c3-8587-d4e530c948f0\r\nAccept-Encoding: gzip\r\n\r\n--297611a6-b583-45c3-8587-d4e530c948f0\r\nContent-Disposition: form-data; name=\"from\"\r\n\r\nRyan Lewis <ticket+9688d21d-95aa-4bed-afc7-f31b35731a3d@tickets.rapidpro.io>\r\n--297611a6-b583-45c3-8587-d4e530c948f0\r\nContent-Disposition: form-data; name=\"to\"\r\n\r\nbob@acme.com\r\n--297611a6-b583-45c3-8587-d4e530c948f0\r\nContent-Disposition: form-data; name=\"subject\"\r\n\r\n[-Tickets] Need help\r\n--297611a6-b583-45c3-8587-d4e530c948f0\r\nContent-Disposition: form-data; name=\"text\"\r\n\r\nWhere are my cookies?\n\n------------------------------------------------\n* Reply to the contact by replying to this email\n* Close this ticket by replying with CLOSE\n* View this contact at http://app.rapidpro.io/contact/read/5d76d86b-3bb9-4d5a-b822-c9d86f5d8e4f/\n\r\n--297611a6-b583-45c3-8587-d4e530c948f0--\r\n", logger.Logs[0].Request)

	dbTicket := models.NewTicket(ticket.UUID, models.Org1, models.CathyID, models.MailgunID, "", "Need help", "Where are my cookies?", map[string]interface{}{
		"contact-uuid":    string(models.CathyUUID),
		"contact-display": "Cathy",
	})

	logger = &flows.HTTPLogger{}
	err = svc.Forward(dbTicket, flows.MsgUUID("ca5607f0-cba8-4c94-9cd5-c4fbc24aa767"), "It's urgent", logger.Log)

	assert.NoError(t, err)
	assert.Equal(t, 1, len(logger.Logs))
	assert.Equal(t, "POST /v3/tickets.rapidpro.io/messages HTTP/1.1\r\nHost: api.mailgun.net\r\nUser-Agent: Go-http-client/1.1\r\nContent-Length: 851\r\nAuthorization: Basic ****************\r\nContent-Type: multipart/form-data; boundary=13e96d5a-4e65-4f07-9189-9d6270c6f3c0\r\nAccept-Encoding: gzip\r\n\r\n--13e96d5a-4e65-4f07-9189-9d6270c6f3c0\r\nContent-Disposition: form-data; name=\"from\"\r\n\r\nCathy <ticket+9688d21d-95aa-4bed-afc7-f31b35731a3d@tickets.rapidpro.io>\r\n--13e96d5a-4e65-4f07-9189-9d6270c6f3c0\r\nContent-Disposition: form-data; name=\"to\"\r\n\r\nbob@acme.com\r\n--13e96d5a-4e65-4f07-9189-9d6270c6f3c0\r\nContent-Disposition: form-data; name=\"subject\"\r\n\r\n[-Tickets] Need help\r\n--13e96d5a-4e65-4f07-9189-9d6270c6f3c0\r\nContent-Disposition: form-data; name=\"text\"\r\n\r\nIt's urgent\n\n------------------------------------------------\n* Reply to the contact by replying to this email\n* Close this ticket by replying with CLOSE\n* View this contact at http://app.rapidpro.io/contact/read/6393abc0-283d-4c9b-a1b3-641a035c34bf/\n\r\n--13e96d5a-4e65-4f07-9189-9d6270c6f3c0\r\nContent-Disposition: form-data; name=\"h:In-Reply-To\"\r\n\r\n\r\n--13e96d5a-4e65-4f07-9189-9d6270c6f3c0--\r\n", logger.Logs[0].Request)
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
			"url_base":   "http://app.rapidpro.io",
		},
	)
	require.NoError(t, err)

	logger := &flows.HTTPLogger{}
	ticket1 := models.NewTicket("88bfa1dc-be33-45c2-b469-294ecb0eba90", models.Org1, models.CathyID, models.ZendeskID, "12", "New ticket", "Where my cookies?", nil)
	ticket2 := models.NewTicket("645eee60-7e84-4a9e-ade3-4fce01ae28f1", models.Org1, models.BobID, models.ZendeskID, "14", "Second ticket", "Where my shoes?", nil)

	err = svc.Close([]*models.Ticket{ticket1, ticket2}, logger.Log)

	assert.NoError(t, err)
	assert.Equal(t, "POST /v3/tickets.rapidpro.io/messages HTTP/1.1\r\nHost: api.mailgun.net\r\nUser-Agent: Go-http-client/1.1\r\nContent-Length: 737\r\nAuthorization: Basic ****************\r\nContent-Type: multipart/form-data; boundary=e7187099-7d38-4f60-955c-325957214c42\r\nAccept-Encoding: gzip\r\n\r\n--e7187099-7d38-4f60-955c-325957214c42\r\nContent-Disposition: form-data; name=\"from\"\r\n\r\nticket+88bfa1dc-be33-45c2-b469-294ecb0eba90@tickets.rapidpro.io\r\n--e7187099-7d38-4f60-955c-325957214c42\r\nContent-Disposition: form-data; name=\"to\"\r\n\r\nbob@acme.com\r\n--e7187099-7d38-4f60-955c-325957214c42\r\nContent-Disposition: form-data; name=\"subject\"\r\n\r\n[-Tickets] New ticket CLOSED\r\n--e7187099-7d38-4f60-955c-325957214c42\r\nContent-Disposition: form-data; name=\"text\"\r\n\r\n\n* Ticket has been closed\n* Replying to the contact will reopen this ticket\n* View this contact at http://app.rapidpro.io/contact/read//\n\r\n--e7187099-7d38-4f60-955c-325957214c42\r\nContent-Disposition: form-data; name=\"h:In-Reply-To\"\r\n\r\n\r\n--e7187099-7d38-4f60-955c-325957214c42--\r\n", logger.Logs[0].Request)

	err = svc.Reopen([]*models.Ticket{ticket2}, logger.Log)

	assert.NoError(t, err)
	assert.Equal(t, "POST /v3/tickets.rapidpro.io/messages HTTP/1.1\r\nHost: api.mailgun.net\r\nUser-Agent: Go-http-client/1.1\r\nContent-Length: 740\r\nAuthorization: Basic ****************\r\nContent-Type: multipart/form-data; boundary=59d74b86-3e2f-4a93-aece-b05d2fdcde0c\r\nAccept-Encoding: gzip\r\n\r\n--59d74b86-3e2f-4a93-aece-b05d2fdcde0c\r\nContent-Disposition: form-data; name=\"from\"\r\n\r\nticket+645eee60-7e84-4a9e-ade3-4fce01ae28f1@tickets.rapidpro.io\r\n--59d74b86-3e2f-4a93-aece-b05d2fdcde0c\r\nContent-Disposition: form-data; name=\"to\"\r\n\r\nbob@acme.com\r\n--59d74b86-3e2f-4a93-aece-b05d2fdcde0c\r\nContent-Disposition: form-data; name=\"subject\"\r\n\r\n[-Tickets] Second ticket CLOSED\r\n--59d74b86-3e2f-4a93-aece-b05d2fdcde0c\r\nContent-Disposition: form-data; name=\"text\"\r\n\r\n\n* Ticket has been closed\n* Replying to the contact will reopen this ticket\n* View this contact at http://app.rapidpro.io/contact/read//\n\r\n--59d74b86-3e2f-4a93-aece-b05d2fdcde0c\r\nContent-Disposition: form-data; name=\"h:In-Reply-To\"\r\n\r\n\r\n--59d74b86-3e2f-4a93-aece-b05d2fdcde0c--\r\n", logger.Logs[1].Request)
}
