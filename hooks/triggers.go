package hooks

import (
	"context"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/mailroom/models"
)

// DeleteTriggerForContactEvent is our hook to remove triggers for contact
type DeleteTriggerForContactEvent struct{}

var deleteTriggerForContactEvent = &DeleteTriggerForContactEvent{}

// Apply will update all the campaigns for the passed in scene, minimizing the number of queries to do so
func (h *DeleteTriggerForContactEvent) Apply(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, scenes map[*models.Scene][]interface{}) error {
	// build up our list of contact id
	contactIDs := make([]interface{}, 0, len(scenes))
	for scene := range scenes {
		contactIDs = append(contactIDs, scene.ContactID())
	}

	// do our update
	return models.BulkSQL(ctx, "removing contact triggers", tx, deleteAllContactTriggersSQL, contactIDs)
}

const deleteAllContactTriggersSQL = `
DELETE FROM
	triggers_trigger_contacts
WHERE
	contact_id = ANY($1)
`
