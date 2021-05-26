package models_test

import (
	"testing"
	"time"

	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
)

func TestAirtimeTransfers(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()

	// insert a transfer
	transfer := models.NewAirtimeTransfer(
		testdata.Org1.ID,
		models.AirtimeTransferStatusSuccess,
		testdata.Cathy.ID,
		urns.URN("tel:+250700000001"),
		urns.URN("tel:+250700000002"),
		"RWF",
		decimal.RequireFromString(`1100`),
		decimal.RequireFromString(`1000`),
		time.Now(),
	)
	err := models.InsertAirtimeTransfers(ctx, db, []*models.AirtimeTransfer{transfer})
	assert.Nil(t, err)

	testsuite.AssertQueryCount(t, db,
		`SELECT count(*) from airtime_airtimetransfer WHERE org_id = $1 AND status = $2`,
		[]interface{}{testdata.Org1.ID, models.AirtimeTransferStatusSuccess}, 1)

	// insert a failed transfer with nil sender, empty currency
	transfer = models.NewAirtimeTransfer(
		testdata.Org1.ID,
		models.AirtimeTransferStatusFailed,
		testdata.Cathy.ID,
		urns.NilURN,
		urns.URN("tel:+250700000002"),
		"",
		decimal.Zero,
		decimal.Zero,
		time.Now(),
	)
	err = models.InsertAirtimeTransfers(ctx, db, []*models.AirtimeTransfer{transfer})
	assert.Nil(t, err)

	testsuite.AssertQueryCount(t, db,
		`SELECT count(*) from airtime_airtimetransfer WHERE org_id = $1 AND status = $2`,
		[]interface{}{testdata.Org1.ID, models.AirtimeTransferStatusFailed}, 1)
}
