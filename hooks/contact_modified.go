package hooks

import (
	"context"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/mailroom/models"
	"github.com/pkg/errors"
)

// ContactModifiedHook is our hook for contact changes that require an update to modified_on
type ContactModifiedHook struct{}

var contactModifiedHook = &ContactModifiedHook{}

// Apply squashes and updates modified_on on all the contacts passed in
func (h *ContactModifiedHook) Apply(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, scenes map[*models.Scene][]interface{}) error {
	// our list of contact ids
	contactIDs := make([]models.ContactID, 0, len(scenes))
	for scene := range scenes {
		contactIDs = append(contactIDs, scene.ContactID())
	}

	err := models.UpdateContactModifiedOn(ctx, tx, contactIDs)
	if err != nil {
		return errors.Wrapf(err, "error updating modified_on on contacts")
	}

	return nil
}
