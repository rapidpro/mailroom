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
	models.RegisterEventHandler(events.TypeContactStopped, handleContactStopped)
}

// CommitContactStoppedHook is our hook for contact stopped
type CommitContactStoppedHook struct{}

var commitContactStoppedHook = &CommitContactStoppedHook{}

// Apply commits our contact is_stopped change
func (h *CommitContactStoppedHook) Apply(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, scenes map[*models.Scene][]interface{}) error {

	// build up our list of contact id
	contactIDs := make([]models.ContactID, 0, len(scenes))
	for scene := range scenes {
		contactIDs = append(contactIDs, scene.ContactID())
	}

	err := models.StopContacts(ctx, tx, contactIDs)
	if err != nil {
		return errors.Wrapf(err, "error stopping contacts")
	}
	return nil
}

// handleContactStopped stops contact
func handleContactStopped(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, scene *models.Scene, e flows.Event) error {
	event := e.(*events.ContactStoppedEvent)
	logrus.WithFields(logrus.Fields{
		"contact_uuid": scene.ContactUUID(),
		"session_id":   scene.SessionID(),
	}).Debug("stopping contact")

	scene.AppendToEventPreCommitHook(commitContactStoppedHook, event)
	return nil
}
