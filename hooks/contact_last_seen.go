package hooks

import (
	"context"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/mailroom/models"
	"github.com/pkg/errors"
)

// ContactLastSeenHook is our hook for contact changes that require an update to last_seen_on
type ContactLastSeenHook struct{}

var contactLastSeenHook = &ContactLastSeenHook{}

// Apply squashes and updates modified_on on all the contacts passed in
func (h *ContactLastSeenHook) Apply(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, oa *models.OrgAssets, scenes map[*models.Scene][]interface{}) error {

	for scene, evts := range scenes {
		lastSeenOn := evts[len(evts)-1].(time.Time)

		err := models.UpdateContactLastSeenOn(ctx, tx, scene.ContactID(), lastSeenOn)
		if err != nil {
			return errors.Wrapf(err, "error updating last_seen_on on contacts")
		}
	}

	return nil
}
