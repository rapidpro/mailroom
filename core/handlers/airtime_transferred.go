package handlers

import (
	"context"
	"time"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/core/hooks"
	"github.com/nyaruka/mailroom/core/models"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/shopspring/decimal"
	"github.com/sirupsen/logrus"
)

func init() {
	models.RegisterEventHandler(events.TypeAirtimeTransferred, handleAirtimeTransferred)
}

// handleAirtimeTransferred is called for each airtime transferred event
func handleAirtimeTransferred(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, oa *models.OrgAssets, scene *models.Scene, e flows.Event) error {
	event := e.(*events.AirtimeTransferredEvent)

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
			oa.OrgID(),
			httpLog.URL,
			httpLog.Request,
			httpLog.Response,
			httpLog.Status != flows.CallStatusSuccess,
			time.Duration(httpLog.ElapsedMS)*time.Millisecond,
			httpLog.CreatedOn,
		))
	}

	scene.AppendToEventPreCommitHook(hooks.InsertAirtimeTransfersHook, transfer)

	return nil
}
