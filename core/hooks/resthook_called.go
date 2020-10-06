package hooks

import (
	"context"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func init() {
	models.RegisterEventHandler(events.TypeResthookCalled, handleResthookCalled)
}

// InsertWebhookEventHook is our hook for when a resthook needs to be inserted
var InsertWebhookEventHook models.EventCommitHook = &insertWebhookEventHook{}

type insertWebhookEventHook struct{}

// Apply inserts all the webook events that were created
func (h *insertWebhookEventHook) Apply(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, oa *models.OrgAssets, scenes map[*models.Scene][]interface{}) error {
	events := make([]*models.WebhookEvent, 0, len(scenes))
	for _, rs := range scenes {
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

// handleResthookCalled is called for each resthook call in a scene
func handleResthookCalled(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, oa *models.OrgAssets, scene *models.Scene, e flows.Event) error {
	event := e.(*events.ResthookCalledEvent)
	logrus.WithFields(logrus.Fields{
		"contact_uuid": scene.ContactUUID(),
		"session_id":   scene.SessionID(),
		"resthook":     event.Resthook,
	}).Debug("resthook called")

	// look up our resthook id
	resthook := oa.ResthookBySlug(event.Resthook)
	if resthook == nil {
		logrus.WithField("org_id", oa.OrgID()).WithField("resthook", event.Resthook).Errorf("unable to find resthook with slug, ignoring event")
		return nil
	}

	// create an event for this call
	re := models.NewWebhookEvent(
		oa.OrgID(),
		resthook.ID(),
		string(event.Payload),
		event.CreatedOn(),
	)
	scene.AppendToEventPreCommitHook(InsertWebhookEventHook, re)

	return nil
}
