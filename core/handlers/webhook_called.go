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
	"github.com/sirupsen/logrus"
)

func init() {
	models.RegisterEventHandler(events.TypeWebhookCalled, handleWebhookCalled)
}

// handleWebhookCalled is called for each webhook call in a scene
func handleWebhookCalled(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, oa *models.OrgAssets, scene *models.Scene, e flows.Event) error {
	event := e.(*events.WebhookCalledEvent)
	logrus.WithFields(logrus.Fields{
		"contact_uuid": scene.ContactUUID(),
		"session_id":   scene.SessionID(),
		"url":          event.URL,
		"status":       event.Status,
		"elapsed_ms":   event.ElapsedMS,
		"resthook":     event.Resthook,
	}).Debug("webhook called")

	// if this was a resthook and the status was 410, that means we should remove it
	if event.Status == flows.CallStatusSubscriberGone {
		unsub := &models.ResthookUnsubscribe{
			OrgID: oa.OrgID(),
			Slug:  event.Resthook,
			URL:   event.URL,
		}

		scene.AppendToEventPreCommitHook(hooks.UnsubscribeResthookHook, unsub)
	}

	// if this is a connection error, use that as our response
	response := event.Response
	if event.Status == flows.CallStatusConnectionError {
		response = "connection error"
	}

	// create a result for this call
	result := models.NewWebhookResult(
		oa.OrgID(), scene.ContactID(),
		event.URL, event.Request,
		event.StatusCode, response,
		time.Millisecond*time.Duration(event.ElapsedMS), event.CreatedOn(),
	)
	scene.AppendToEventPreCommitHook(hooks.InsertWebhookResultHook, result)

	return nil
}
