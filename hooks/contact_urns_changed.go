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
	models.RegisterEventHook(events.TypeContactURNsChanged, handleContactURNsChanged)
}

// ContactURNsChangedHook is our hook for when a URN is added to a contact
type ContactURNsChangedHook struct{}

var contactURNsChangedHook = &ContactURNsChangedHook{}

// Apply adds all our URNS in a batch
func (h *ContactURNsChangedHook) Apply(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, sessions map[*models.Session][]interface{}) error {
	// gather all our urn changes, we only care about the last change for each session
	changes := make([]*models.ContactURNsChanged, 0, len(sessions))
	for _, sessionChanges := range sessions {
		changes = append(changes, sessionChanges[len(sessionChanges)-1].(*models.ContactURNsChanged))
	}

	err := models.UpdateContactURNs(ctx, tx, org, changes)
	if err != nil {
		return errors.Wrapf(err, "error updating contact urns")
	}

	return nil
}

// handleContactURNsChanged is called for each contact urn changed event that is encountered
func handleContactURNsChanged(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, session *models.Session, e flows.Event) error {
	event := e.(*events.ContactURNsChangedEvent)
	logrus.WithFields(logrus.Fields{
		"contact_uuid": session.ContactUUID(),
		"session_id":   session.ID,
		"urns":         event.URNs,
	}).Debug("contact urns changed")

	// create our URN changed event
	change := &models.ContactURNsChanged{
		ContactID: session.Contact().ID(),
		OrgID:     org.OrgID(),
		URNs:      event.URNs,
	}

	// add our callback
	session.AddPreCommitEvent(contactURNsChangedHook, change)
	session.AddPreCommitEvent(contactModifiedHook, session.Contact().ID())

	return nil
}
