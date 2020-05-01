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
	models.RegisterEventHandler(events.TypeContactStatusChanged, handleContactStatusChanged)
}

// CommitStatusChangesHook is our hook for status changes
type CommitStatusChangesHook struct{}

var commitStatusChangesHook = &CommitStatusChangesHook{}

// Apply commits our contact status change
func (h *CommitStatusChangesHook) Apply(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, scenes map[*models.Scene][]interface{}) error {

	statusChanges := make([]*models.ContactStatusChange, 0, len(scenes))
	for scene, es := range scenes {

		event := es[len(es)-1].(*events.ContactStatusChangedEvent)
		statusChanges = append(statusChanges, &models.ContactStatusChange{ContactID: scene.ContactID(), Status: event.Status})
	}

	err := models.UpdateContactStatus(ctx, tx, statusChanges)
	if err != nil {
		return errors.Wrapf(err, "error updating contact statuses")
	}
	return nil
}

// handleContactStatusChanged updates contact status
func handleContactStatusChanged(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, scene *models.Scene, e flows.Event) error {
	event := e.(*events.ContactStatusChangedEvent)
	logrus.WithFields(logrus.Fields{
		"contact_uuid": scene.ContactUUID(),
		"session_id":   scene.SessionID(),
		"status":       event.Status,
	}).Debug("updating contact status")

	scene.AppendToEventPreCommitHook(commitStatusChangesHook, event)
	return nil
}
