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
	models.RegisterEventHook(events.TypeAirtimeTransferred, handleAirtimeTransferred)
}

// InsertAirtimeTransfersHook is our hook for inserting airtime transfers
type InsertAirtimeTransfersHook struct{}

var insertAirtimeTransfersHook = &InsertAirtimeTransfersHook{}

// Apply inserts all the airtime transfers that were created
func (h *InsertAirtimeTransfersHook) Apply(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, sessions map[*models.Session][]interface{}) error {
	// gather all our transfers and logs
	transfers := make([]*models.AirtimeTransfer, 0, len(sessions))
	logs := make([]*models.HTTPLog, 0, len(sessions))
	logsByTransfer := make(map[*models.AirtimeTransfer][]*models.HTTPLog, len(sessions))

	for _, ts := range sessions {
		for _, t := range ts {
			transfer := t.(*models.AirtimeTransfer)
			transfers = append(transfers, transfer)

			for _, l := range transfer.Logs {
				logs = append(logs, l)
				logsByTransfer[transfer] = append(logsByTransfer[transfer], l)
			}
		}
	}

	// insert the transfers
	err := models.InsertAirtimeTransfers(ctx, tx, transfers)
	if err != nil {
		return errors.Wrapf(err, "error inserting airtime transfers")
	}

	// set the newly inserted transfer IDs on the logs
	for t, ls := range logsByTransfer {
		for _, l := range ls {
			l.SetAirtimeTransferID(t.ID())
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
func handleAirtimeTransferred(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, session *models.Session, e flows.Event) error {
	event := e.(*events.AirtimeTransferredEvent)

	status := models.AirtimeTransferStatusSuccess
	if event.ActualAmount == decimal.Zero {
		status = models.AirtimeTransferStatusFailed
	}

	transfer := models.NewAirtimeTransfer(
		org.OrgID(),
		status,
		session.ContactID(),
		event.Sender,
		event.Recipient,
		event.Currency,
		event.DesiredAmount,
		event.ActualAmount,
		event.CreatedOn(),
	)

	logrus.WithFields(logrus.Fields{
		"contact_uuid":   session.ContactUUID(),
		"session_id":     session.ID(),
		"sender":         string(event.Sender),
		"recipient":      string(event.Recipient),
		"currency":       event.Currency,
		"desired_amount": event.DesiredAmount.String(),
		"actual_amount":  event.ActualAmount.String(),
	}).Debug("airtime transferred")

	// add a log for each HTTP call
	for _, httpLog := range event.HTTPLogs {
		transfer.AddLog(models.NewHTTPLog(
			org.OrgID(),
			httpLog.URL,
			httpLog.Request,
			httpLog.Response,
			httpLog.Status != flows.CallStatusSuccess,
			time.Duration(httpLog.ElapsedMS)*time.Millisecond,
			httpLog.CreatedOn,
		))
	}

	session.AddPreCommitEvent(insertAirtimeTransfersHook, transfer)

	return nil
}
