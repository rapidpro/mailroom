package hooks

import (
	"context"
	"time"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/models"
	"github.com/shopspring/decimal"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func init() {
	models.RegisterEventHandler(events.TypeAirtimeTransferred, handleAirtimeTransferred)
}

// InsertAirtimeTransfersHook is our hook for inserting airtime transfers
type InsertAirtimeTransfersHook struct{}

var insertAirtimeTransfersHook = &InsertAirtimeTransfersHook{}

// Apply inserts all the airtime transfers that were created
func (h *InsertAirtimeTransfersHook) Apply(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, scenes map[*models.Scene][]interface{}) error {
	// gather all our transfers
	transfers := make([]*models.AirtimeTransfer, 0, len(scenes))

	for _, ts := range scenes {
		for _, t := range ts {
			transfer := t.(*models.AirtimeTransfer)
			transfers = append(transfers, transfer)
		}
	}

	// insert the transfers
	err := models.InsertAirtimeTransfers(ctx, tx, transfers)
	if err != nil {
		return errors.Wrapf(err, "error inserting airtime transfers")
	}

	// gather all our logs and set the newly inserted transfer IDs on them
	logs := make([]*models.HTTPLog, 0, len(scenes))

	for _, t := range transfers {
		for _, l := range t.Logs {
			l.SetAirtimeTransferID(t.ID())
			logs = append(logs, l)
		}
	}

	// insert the logs
	err = models.InsertHTTPLogs(ctx, tx, logs)
	if err != nil {
		return errors.Wrapf(err, "error inserting airtime transfer logs")
	}

	return nil
}

// handleAirtimeTransferred is called for each airtime transferred event
func handleAirtimeTransferred(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, scene *models.Scene, e flows.Event) error {
	event := e.(*events.AirtimeTransferredEvent)

	status := models.AirtimeTransferStatusSuccess
	if event.ActualAmount == decimal.Zero {
		status = models.AirtimeTransferStatusFailed
	}

	transfer := models.NewAirtimeTransfer(
		org.OrgID(),
		status,
		scene.ContactID(),
		event.Sender,
		event.Recipient,
		event.Currency,
		event.DesiredAmount,
		event.ActualAmount,
		event.CreatedOn(),
	)

	logrus.WithFields(logrus.Fields{
		"contact_uuid":   scene.ContactUUID(),
		"session_id":     scene.SessionID(),
		"sender":         string(event.Sender),
		"recipient":      string(event.Recipient),
		"currency":       event.Currency,
		"desired_amount": event.DesiredAmount.String(),
		"actual_amount":  event.ActualAmount.String(),
	}).Debug("airtime transferred")

	// add a log for each HTTP call
	for _, httpLog := range event.HTTPLogs {
		transfer.AddLog(models.NewAirtimeTransferredLog(
			org.OrgID(),
			httpLog.URL,
			httpLog.Request,
			httpLog.Response,
			httpLog.Status != flows.CallStatusSuccess,
			time.Duration(httpLog.ElapsedMS)*time.Millisecond,
			httpLog.CreatedOn,
		))
	}

	scene.AppendToEventPreCommitHook(insertAirtimeTransfersHook, transfer)

	return nil
}
