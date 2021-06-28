package models

import (
	"testing"
	"time"

	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/mailroom/testsuite"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
)

func TestAirtimeTransfers(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()

	// insert a transfer
	transfer := NewAirtimeTransfer(
		Org1,
		AirtimeTransferStatusSuccess,
		CathyID,
		urns.URN("tel:+250700000001"),
		urns.URN("tel:+250700000002"),
		"RWF",
		decimal.RequireFromString(`1100`),
		decimal.RequireFromString(`1000`),
		time.Now(),
	)
	err := InsertAirtimeTransfers(ctx, db, []*AirtimeTransfer{transfer})
	assert.Nil(t, err)

	testsuite.AssertQueryCount(t, db,
		`SELECT count(*) from airtime_airtimetransfer WHERE org_id = $1 AND status = $2`,
		[]interface{}{Org1, AirtimeTransferStatusSuccess}, 1)

	// insert a failed transfer with nil sender, empty currency
	transfer = NewAirtimeTransfer(
		Org1,
		AirtimeTransferStatusFailed,
		CathyID,
		urns.NilURN,
		urns.URN("tel:+250700000002"),
		"",
		decimal.Zero,
		decimal.Zero,
		time.Now(),
	)
	err = InsertAirtimeTransfers(ctx, db, []*AirtimeTransfer{transfer})
	assert.Nil(t, err)

	testsuite.AssertQueryCount(t, db,
		`SELECT count(*) from airtime_airtimetransfer WHERE org_id = $1 AND status = $2`,
		[]interface{}{Org1, AirtimeTransferStatusFailed}, 1)
}
