package hooks

import (
	"context"

	"github.com/nyaruka/mailroom/models"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

// ContactModifiedHook is our hook for contact changes that require an update to modified_on
type ContactModifiedHook struct{}

var contactModifiedHook = &ContactModifiedHook{}

type modifiedEvent struct {
	InputReceived bool
}

// Apply squashes and updates modified_on on all the contacts passed in
func (h *ContactModifiedHook) Apply(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, oa *models.OrgAssets, scenes map[*models.Scene][]interface{}) error {
	// our lists of contact ids
	contactIDsWithInput := make([]models.ContactID, 0, len(scenes))
	contactIDsWithoutInput := make([]models.ContactID, 0, len(scenes))

	for scene, opts := range scenes {
		hasInput := false
		for _, o := range opts {
			if o.(modifiedEvent).InputReceived {
				hasInput = true
				break
			}
		}

		if hasInput {
			contactIDsWithInput = append(contactIDsWithInput, scene.ContactID())
		} else {
			contactIDsWithoutInput = append(contactIDsWithoutInput, scene.ContactID())
		}
	}

	if len(contactIDsWithInput) > 0 {
		err := models.UpdateContactLastSeenOn(ctx, tx, contactIDsWithInput)
		if err != nil {
			return errors.Wrapf(err, "error updating last_seen_on on contacts")
		}
	}
	if len(contactIDsWithoutInput) > 0 {
		err := models.UpdateContactModifiedOn(ctx, tx, contactIDsWithoutInput)
		if err != nil {
			return errors.Wrapf(err, "error updating modified_on on contacts")
		}
	}

	return nil
}
