package goflow_test

import (
	"net/http"
	"testing"

	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/utils/httpx"
	"github.com/nyaruka/mailroom/goflow"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/testsuite"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEngineWebhook(t *testing.T) {
	svc, err := goflow.Engine().Services().Webhook(nil)
	assert.NoError(t, err)

	defer httpx.SetRequestor(httpx.DefaultRequestor)
	httpx.SetRequestor(httpx.NewMockRequestor(map[string][]httpx.MockResponse{
		"http://rapidpro.io": {httpx.NewMockResponse(200, nil, "OK")},
	}))

	request, err := http.NewRequest("GET", "http://rapidpro.io", nil)
	require.NoError(t, err)

	call, err := svc.Call(nil, request)
	assert.NoError(t, err)
	assert.NotNil(t, call)
	assert.Equal(t, "GET / HTTP/1.1\r\nHost: rapidpro.io\r\nUser-Agent: RapidProMailroom/Dev\r\nX-Mailroom-Mode: normal\r\nAccept-Encoding: gzip\r\n\r\n", string(call.RequestTrace))
	assert.Equal(t, "HTTP/1.0 200 OK\r\nContent-Length: 2\r\n\r\n", string(call.ResponseTrace))
	assert.Equal(t, "OK", string(call.ResponseBody))
}

func TestSimulatorAirtime(t *testing.T) {
	svc, err := goflow.Simulator().Services().Airtime(nil)
	assert.NoError(t, err)

	amounts := map[string]decimal.Decimal{"USD": decimal.RequireFromString(`1.50`)}

	transfer, err := svc.Transfer(nil, urns.URN("tel:+593979111111"), urns.URN("tel:+593979222222"), amounts, nil)
	assert.NoError(t, err)

	assert.Equal(t, &flows.AirtimeTransfer{
		Sender:        urns.URN("tel:+593979111111"),
		Recipient:     urns.URN("tel:+593979222222"),
		Currency:      "USD",
		DesiredAmount: decimal.RequireFromString(`1.50`),
		ActualAmount:  decimal.RequireFromString(`1.50`),
	}, transfer)
}

func TestSimulatorTicket(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()
	testsuite.ResetDB()

	ticketer, err := models.LookupTicketerByUUID(ctx, db, models.MailgunUUID)
	require.NoError(t, err)

	svc, err := goflow.Simulator().Services().Ticket(nil, flows.NewTicketer(ticketer))
	assert.NoError(t, err)

	ticket, err := svc.Open(nil, "New ticket", "Where are my cookies?", nil)
	assert.NoError(t, err)
	assert.Equal(t, models.MailgunUUID, ticket.Ticketer.UUID)
	assert.Equal(t, "New ticket", ticket.Subject)
	assert.Equal(t, "Where are my cookies?", ticket.Body)
}

func TestSimulatorWebhook(t *testing.T) {
	svc, err := goflow.Simulator().Services().Webhook(nil)
	assert.NoError(t, err)

	defer httpx.SetRequestor(httpx.DefaultRequestor)
	httpx.SetRequestor(httpx.NewMockRequestor(map[string][]httpx.MockResponse{
		"http://rapidpro.io": {httpx.NewMockResponse(200, nil, "OK")},
	}))

	request, err := http.NewRequest("GET", "http://rapidpro.io", nil)
	require.NoError(t, err)

	call, err := svc.Call(nil, request)
	assert.NoError(t, err)
	assert.NotNil(t, call)
	assert.Equal(t, "GET / HTTP/1.1\r\nHost: rapidpro.io\r\nUser-Agent: RapidProMailroom/Dev\r\nX-Mailroom-Mode: simulation\r\nAccept-Encoding: gzip\r\n\r\n", string(call.RequestTrace))
	assert.Equal(t, "HTTP/1.0 200 OK\r\nContent-Length: 2\r\n\r\n", string(call.ResponseTrace))
	assert.Equal(t, "OK", string(call.ResponseBody))
}
