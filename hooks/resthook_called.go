package hooks

import (
	"context"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/models"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func init() {
	models.RegisterEventHook(events.TypeResthookCalled, handleResthookCalled)
}

// InsertWebhookEventHook is our hook for when a resthook needs to be inserted
type InsertWebhookEventHook struct{}

var insertWebhookEventHook = &InsertWebhookEventHook{}

// Apply inserts all the webook events that were created
func (h *InsertWebhookEventHook) Apply(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, sessions map[*models.Session][]interface{}) error {
	events := make([]*models.WebhookEvent, 0, len(sessions))
	for _, rs := range sessions {
		for _, r := range rs {
			events = append(events, r.(*models.WebhookEvent))
		}
	}

	err := models.InsertWebhookEvents(ctx, tx, events)
	if err != nil {
		return errors.Wrapf(err, "error inserting webhook events")
	}

	return nil
}

// handleResthookCalled is called for each resthook call in a session
func handleResthookCalled(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, session *models.Session, e flows.Event) error {
	event := e.(*events.ResthookCalledEvent)
	logrus.WithFields(logrus.Fields{
		"contact_uuid": session.ContactUUID(),
		"session_id":   session.ID(),
		"resthook":     event.Resthook,
	}).Debug("resthook called")

	// look up our resthook id
	resthook := org.ResthookBySlug(event.Resthook)
	if resthook == nil {
		logrus.WithField("org_id", org.OrgID()).WithField("resthook", event.Resthook).Errorf("unable to find resthook with slug, ignoring event")
		return nil
	}

	// create an event for this call
	re := models.NewWebhookEvent(
		org.OrgID(),
		resthook.ID(),
		string(event.Payload),
		event.CreatedOn(),
	)
	session.AddPreCommitEvent(insertWebhookEventHook, re)

	return nil
}
