package hooks

import (
	"context"

	"github.com/nyaruka/mailroom/core/models"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

// ContactModifiedHook is our hook for contact changes that require an update to modified_on
var ContactModifiedHook models.EventCommitHook = &contactModifiedHook{}

type contactModifiedHook struct{}

// Apply squashes and updates modified_on on all the contacts passed in
func (h *contactModifiedHook) Apply(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, oa *models.OrgAssets, scenes map[*models.Scene][]interface{}) error {
	// our lists of contact ids
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
