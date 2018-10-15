package hooks

import (
	"context"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/juju/errors"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/models"
	"github.com/sirupsen/logrus"
)

func init() {
	models.RegisterEventHook(events.TypeContactURNAdded, handleContactURNAdded)
}

// ContactURNAddedHook is our hook for when a URN is added to a contact
type ContactURNAddedHook struct{}

var contactURNAddedHook = &ContactURNAddedHook{}

// Apply squashes and delete all our contact groups
func (h *ContactURNAddedHook) Apply(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, sessions map[*models.Session][]interface{}) error {
	// gather all our contact urn adds
	urnAdds := make([]*models.ContactURNAdd, 0, len(sessions))
	for _, adds := range sessions {
		for _, add := range adds {
			urnAdds = append(urnAdds, add.(*models.ContactURNAdd))
		}
	}

	err := models.AddContactURNs(ctx, tx, urnAdds)
	if err != nil {
		return errors.Annotatef(err, "error adding urns to contacts")
	}

	return nil
}

// handleContactURNAdded is called for each contact urn added event encountered in a session
func handleContactURNAdded(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, session *models.Session, e flows.Event) error {
	event := e.(*events.ContactURNAddedEvent)
	logrus.WithFields(logrus.Fields{
		"contact_uuid": session.ContactUUID(),
		"session_id":   session.ID,
		"urn":          event.URN,
	}).Debug("contact urn added")

	// create our URN add
	urnAdd := &models.ContactURNAdd{
		ContactID: session.Contact().ID(),
		OrgID:     org.OrgID(),
		Identity:  event.URN.Identity().String(),
		Path:      event.URN.Path(),
		Scheme:    event.URN.Scheme(),
		Priority:  -1,
	}

	// add our callback
	session.AddPreCommitEvent(contactURNAddedHook, urnAdd)

	return nil
}
