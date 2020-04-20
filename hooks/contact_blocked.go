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
	models.RegisterEventHandler(events.TypeContactBlocked, handleContactBlocked)
}

// CommitContactBlockedHook is our hook for contact blocked
type CommitContactBlockedHook struct{}

var commitContactBlockedHook = &CommitContactBlockedHook{}

// Apply commits our contact is_blocked change
func (h *CommitContactBlockedHook) Apply(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, scenes map[*models.Scene][]interface{}) error {

	// build up our list of contact id
	contactIDs := make([]models.ContactID, 0, len(scenes))
	for scene := range scenes {
		contactIDs = append(contactIDs, scene.ContactID())
	}

	err := models.BlockContacts(ctx, tx, contactIDs)
	if err != nil {
		return errors.Wrapf(err, "error blocking contacts")
	}
	return nil
}

// handleContactBlocked blocks contact
func handleContactBlocked(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, scene *models.Scene, e flows.Event) error {
	event := e.(*events.ContactBlockedEvent)
	logrus.WithFields(logrus.Fields{
		"contact_uuid": scene.ContactUUID(),
		"session_id":   scene.SessionID(),
	}).Debug("blocking contact")

	scene.AppendToEventPreCommitHook(commitContactBlockedHook, event)
	return nil
}
