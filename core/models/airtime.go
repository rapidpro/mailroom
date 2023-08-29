package models

import (
	"context"
	"database/sql/driver"
	"time"

	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/null/v2"
	"github.com/shopspring/decimal"
)

// AirtimeTransferID is the type for airtime transfer IDs
type AirtimeTransferID int

// NilAirtimeTransferID is the nil value for airtime transfer IDs
var NilAirtimeTransferID = AirtimeTransferID(0)

// AirtimeTransferStatus is the type for the status of a transfer
type AirtimeTransferStatus string

const (
	// AirtimeTransferStatusSuccess is our status for successful transfers
	AirtimeTransferStatusSuccess AirtimeTransferStatus = "S"

	// AirtimeTransferStatusFailed is our status for failed transfers
	AirtimeTransferStatusFailed AirtimeTransferStatus = "F"
)

// AirtimeTransfer is our type for an airtime transfer
type AirtimeTransfer struct {
	t struct {
		ID            AirtimeTransferID     `db:"id"`
		OrgID         OrgID                 `db:"org_id"`
		Status        AirtimeTransferStatus `db:"status"`
		ContactID     ContactID             `db:"contact_id"`
		Sender        null.String           `db:"sender"`
		Recipient     urns.URN              `db:"recipient"`
		Currency      null.String           `db:"currency"`
		DesiredAmount decimal.Decimal       `db:"desired_amount"`
		ActualAmount  decimal.Decimal       `db:"actual_amount"`
		CreatedOn     time.Time             `db:"created_on"`
	}

	Logs []*HTTPLog
}

// NewAirtimeTransfer creates a new airtime transfer returning the result
func NewAirtimeTransfer(orgID OrgID, status AirtimeTransferStatus, contactID ContactID, sender urns.URN, recipient urns.URN, currency string, desiredAmount decimal.Decimal, actualAmount decimal.Decimal, createdOn time.Time) *AirtimeTransfer {
	t := &AirtimeTransfer{}
	t.t.OrgID = orgID
	t.t.Status = status
	t.t.ContactID = contactID
	t.t.Sender = null.String(string(sender))
	t.t.Recipient = recipient
	t.t.Currency = null.String(currency)
	t.t.DesiredAmount = desiredAmount
	t.t.ActualAmount = actualAmount
	t.t.CreatedOn = createdOn
	return t
}

func (t *AirtimeTransfer) ID() AirtimeTransferID {
	return t.t.ID
}

func (t *AirtimeTransfer) AddLog(l *HTTPLog) {
	t.Logs = append(t.Logs, l)
}

const sqlInsertAirtimeTransfers = `
INSERT INTO airtime_airtimetransfer(org_id,  status,  contact_id,  sender,  recipient,  currency,  desired_amount,  actual_amount,  created_on)
					        VALUES(:org_id, :status, :contact_id, :sender, :recipient, :currency, :desired_amount, :actual_amount, :created_on)
RETURNING id
`

// InsertAirtimeTransfers inserts the passed in airtime transfers returning any errors encountered
func InsertAirtimeTransfers(ctx context.Context, db DBorTx, transfers []*AirtimeTransfer) error {
	if len(transfers) == 0 {
		return nil
	}

	ts := make([]interface{}, len(transfers))
	for i := range transfers {
		ts[i] = &transfers[i].t
	}

	return BulkQuery(ctx, "inserted airtime transfers", db, sqlInsertAirtimeTransfers, ts)
}

func (i *AirtimeTransferID) Scan(value any) error         { return null.ScanInt(value, i) }
func (i AirtimeTransferID) Value() (driver.Value, error)  { return null.IntValue(i) }
func (i *AirtimeTransferID) UnmarshalJSON(b []byte) error { return null.UnmarshalInt(b, i) }
func (i AirtimeTransferID) MarshalJSON() ([]byte, error)  { return null.MarshalInt(i) }
