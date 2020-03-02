package goflow

import (
	"net/http"
	"testing"

	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/utils/httpx"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEngineWebhook(t *testing.T) {
	svc, err := Engine().Services().Webhook(nil)
	assert.NoError(t, err)

	defer httpx.SetRequestor(httpx.DefaultRequestor)
	httpx.SetRequestor(httpx.NewMockRequestor(map[string][]httpx.MockResponse{
		"http://temba.io": []httpx.MockResponse{httpx.NewMockResponse(200, nil, "OK", 1)},
	}))

	request, err := http.NewRequest("GET", "http://temba.io", nil)
	require.NoError(t, err)

	call, err := svc.Call(nil, request)
	assert.NoError(t, err)
	assert.NotNil(t, call)
	assert.Equal(t, "GET / HTTP/1.1\r\nHost: temba.io\r\nUser-Agent: RapidProMailroom/Dev\r\nX-Mailroom-Mode: normal\r\nAccept-Encoding: gzip\r\n\r\n", string(call.RequestTrace))
	assert.Equal(t, "HTTP/1.0 200 OK\r\nContent-Length: 2\r\n\r\nOK", string(call.ResponseTrace))
}

func TestSimulatorAirtime(t *testing.T) {
	svc, err := Simulator().Services().Airtime(nil)
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

func TestSimulatorWebhook(t *testing.T) {
	svc, err := Simulator().Services().Webhook(nil)
	assert.NoError(t, err)

	defer httpx.SetRequestor(httpx.DefaultRequestor)
	httpx.SetRequestor(httpx.NewMockRequestor(map[string][]httpx.MockResponse{
		"http://temba.io": []httpx.MockResponse{httpx.NewMockResponse(200, nil, "OK", 1)},
	}))

	request, err := http.NewRequest("GET", "http://temba.io", nil)
	require.NoError(t, err)

	call, err := svc.Call(nil, request)
	assert.NoError(t, err)
	assert.NotNil(t, call)
	assert.Equal(t, "GET / HTTP/1.1\r\nHost: temba.io\r\nUser-Agent: RapidProMailroom/Dev\r\nX-Mailroom-Mode: simulation\r\nAccept-Encoding: gzip\r\n\r\n", string(call.RequestTrace))
	assert.Equal(t, "HTTP/1.0 200 OK\r\nContent-Length: 2\r\n\r\nOK", string(call.ResponseTrace))
}
