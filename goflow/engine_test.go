package goflow

import (
	"testing"

	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/flows"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
)

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
