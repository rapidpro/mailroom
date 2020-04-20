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
	models.RegisterEventHandler(events.TypeContactUnblocked, handleContactUnblocked)
}

// CommitContactUnblockedHook is our hook for contact unblocked
type CommitContactUnblockedHook struct{}

var commitContactUnblockedHook = &CommitContactUnblockedHook{}

// Apply commits our contact is_blocked change
func (h *CommitContactUnblockedHook) Apply(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, scenes map[*models.Scene][]interface{}) error {

	// build up our list of contact id
	contactIDs := make([]models.ContactID, 0, len(scenes))
	for scene := range scenes {
		contactIDs = append(contactIDs, scene.ContactID())
	}

	err := models.UnblockContacts(ctx, tx, contactIDs)
	if err != nil {
		return errors.Wrapf(err, "error stopping contacts")
	}
	return nil
}

// handleContactUnblocked unblocks contact
func handleContactUnblocked(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, scene *models.Scene, e flows.Event) error {
	event := e.(*events.ContactUnblockedEvent)
	logrus.WithFields(logrus.Fields{
		"contact_uuid": scene.ContactUUID(),
		"session_id":   scene.SessionID(),
	}).Debug("unblocking contact")

	scene.AppendToEventPreCommitHook(commitContactUnblockedHook, event)
	return nil
}
