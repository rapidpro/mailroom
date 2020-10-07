package handlers

import (
	"context"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/core/hooks"
	"github.com/nyaruka/mailroom/core/models"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"
)

func init() {
	models.RegisterEventHandler(events.TypeResthookCalled, handleResthookCalled)
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
	scene.AppendToEventPreCommitHook(hooks.InsertWebhookEventHook, re)

	return nil
}
