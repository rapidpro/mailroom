package hooks

import (
	"context"
	"strings"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/models"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func init() {
	models.RegisterEventHook(events.TypeWebhookCalled, handleWebhookCalled)
}

// UnsubscribeResthookHook is our hook for when a webhook is called
type UnsubscribeResthookHook struct{}

var unsubscribeResthookHook = &UnsubscribeResthookHook{}

// Apply squashes and applies all our resthook unsubscriptions
func (h *UnsubscribeResthookHook) Apply(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, sessions map[*models.Session][]interface{}) error {
	// gather all our unsubscribes
	unsubs := make([]*models.ResthookUnsubscribe, 0, len(sessions))
	for _, us := range sessions {
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

// handleWebhookCalled is called for each webhook call in a session
func handleWebhookCalled(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, session *models.Session, e flows.Event) error {
	event := e.(*events.WebhookCalledEvent)
	logrus.WithFields(logrus.Fields{
		"contact_uuid": session.ContactUUID(),
		"session_id":   session.ID,
		"url":          event.URL,
		"status":       event.Status,
		"elapsed_ms":   event.ElapsedMS,
		"resthook":     event.Resthook,
	}).Debug("webhook called")

	// if this was a resthook and the status was 410, that means we should remove it
	// TODO: replace with something more sane
	if strings.Contains(event.Response, "410") && event.Resthook != "" {
		unsub := &models.ResthookUnsubscribe{
			OrgID: org.OrgID(),
			Slug:  event.Resthook,
			URL:   event.URL,
		}

		session.AddPreCommitEvent(unsubscribeResthookHook, unsub)
	}

	return nil
}
