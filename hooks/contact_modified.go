package hooks

import (
	"context"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/models"
	"github.com/pkg/errors"
)

// ContactModifiedHook is our hook for contact changes that require an update to modified_on
type ContactModifiedHook struct{}

var contactModifiedHook = &ContactModifiedHook{}

// Apply squashes and updates modified_on on all the contacts passed in
func (h *ContactModifiedHook) Apply(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, sessions map[*models.Session][]interface{}) error {
	// our list of contact ids
	contactIDs := make([]flows.ContactID, 0, len(sessions))
	for session := range sessions {
		contactIDs = append(contactIDs, session.Contact().ID())
	}

	err := models.UpdateContactModifiedOn(ctx, tx, contactIDs)
	if err != nil {
		return errors.Wrapf(err, "error updating modified_on on contacts")
	}

	return nil
}
