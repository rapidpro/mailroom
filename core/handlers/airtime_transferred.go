package handlers

import (
	"context"
	"log/slog"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/core/hooks"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/shopspring/decimal"
)

func init() {
	models.RegisterEventHandler(events.TypeAirtimeTransferred, handleAirtimeTransferred)
}

// handleAirtimeTransferred is called for each airtime transferred event
func handleAirtimeTransferred(ctx context.Context, rt *runtime.Runtime, tx *sqlx.Tx, oa *models.OrgAssets, scene *models.Scene, e flows.Event) error {
	event := e.(*events.AirtimeTransferredEvent)

	slog.Debug("airtime transferred", "contact", scene.ContactUUID(), "session", scene.SessionID(), "sender", event.Sender, "recipient", event.Recipient, "currency", event.Currency, "desired_amount", event.DesiredAmount.String(), "actual_amount", event.ActualAmount.String())

	status := models.AirtimeTransferStatusSuccess
	if event.ActualAmount == decimal.Zero {
		status = models.AirtimeTransferStatusFailed
	}

	transfer := models.NewAirtimeTransfer(
		oa.OrgID(),
		status,
		scene.ContactID(),
		event.Sender,
		event.Recipient,
		event.Currency,
		event.DesiredAmount,
		event.ActualAmount,
		event.CreatedOn(),
	)

	// add a log for each HTTP call
	for _, httpLog := range event.HTTPLogs {
		transfer.AddLog(models.NewAirtimeTransferredLog(
			oa.OrgID(),
			httpLog.URL,
			httpLog.StatusCode,
			httpLog.Request,
			httpLog.Response,
			httpLog.Status != flows.CallStatusSuccess,
			time.Duration(httpLog.ElapsedMS)*time.Millisecond,
			httpLog.Retries,
			httpLog.CreatedOn,
		))
	}

	scene.AppendToEventPreCommitHook(hooks.InsertAirtimeTransfersHook, transfer)

	return nil
}
