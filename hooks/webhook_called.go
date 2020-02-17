package hooks

import (
	"context"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/models"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func init() {
	models.RegisterEventHandler(events.TypeWebhookCalled, handleWebhookCalled)
}

// UnsubscribeResthookHook is our hook for when a webhook is called
type UnsubscribeResthookHook struct{}

var unsubscribeResthookHook = &UnsubscribeResthookHook{}

// Apply squashes and applies all our resthook unsubscriptions
func (h *UnsubscribeResthookHook) Apply(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, scene map[*models.Scene][]interface{}) error {
	// gather all our unsubscribes
	unsubs := make([]*models.ResthookUnsubscribe, 0, len(scene))
	for _, us := range scene {
		for _, u := range us {
			unsubs = append(unsubs, u.(*models.ResthookUnsubscribe))
		}
	}

	err := models.UnsubscribeResthooks(ctx, tx, unsubs)
	if err != nil {
		return errors.Wrapf(err, "error unsubscribing from resthooks")
	}

	return nil
}

// InsertWebhookResultHook is our hook for inserting webhook results
type InsertWebhookResultHook struct{}

var insertWebhookResultHook = &InsertWebhookResultHook{}

// Apply inserts all the webook results that were created
func (h *InsertWebhookResultHook) Apply(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, scenes map[*models.Scene][]interface{}) error {
	// gather all our results
	results := make([]*models.WebhookResult, 0, len(scenes))
	for _, rs := range scenes {
		for _, r := range rs {
			results = append(results, r.(*models.WebhookResult))
		}
	}

	err := models.InsertWebhookResults(ctx, tx, results)
	if err != nil {
		return errors.Wrapf(err, "error inserting webhook results")
	}

	return nil
}

// handleWebhookCalled is called for each webhook call in a scene
func handleWebhookCalled(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, scene *models.Scene, e flows.Event) error {
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
			OrgID: org.OrgID(),
			Slug:  event.Resthook,
			URL:   event.URL,
		}

		scene.AppendToEventPreCommitHook(unsubscribeResthookHook, unsub)
	}

	// if this is a connection error, use that as our response
	response := event.Response
	if event.Status == flows.CallStatusConnectionError {
		response = "connection error"
	}

	// create a result for this call
	result := models.NewWebhookResult(
		org.OrgID(), scene.ContactID(),
		event.URL, event.Request,
		event.StatusCode, response,
		time.Millisecond*time.Duration(event.ElapsedMS), event.CreatedOn(),
	)
	scene.AppendToEventPreCommitHook(insertWebhookResultHook, result)

	return nil
}
