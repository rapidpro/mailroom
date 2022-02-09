package hooks

import (
	"context"

	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
)

// CommitFlowChangesHook is our hook for name changes
var CommitFlowChangesHook models.EventCommitHook = &commitFlowChangesHook{}

type commitFlowChangesHook struct{}

// Apply commits our contact current_flow changes as a bulk update for the passed in map of scene
func (h *commitFlowChangesHook) Apply(ctx context.Context, rt *runtime.Runtime, tx *sqlx.Tx, oa *models.OrgAssets, scenes map[*models.Scene][]interface{}) error {
	// build up our list of pairs of contact id and current flow id
	updates := make([]interface{}, 0, len(scenes))
	for s, evts := range scenes {
		// there is only ever one of these events per scene
		event := evts[len(evts)-1].(*models.ContactFlowChangedEvent)
		updates = append(updates, &currentFlowUpdate{s.ContactID(), event.FlowID})
	}

	// do our update
	return models.BulkQuery(ctx, "updating contact current flow", tx, sqlUpdateContactCurrentFlow, updates)
}

// struct used for our bulk insert
type currentFlowUpdate struct {
	ID            models.ContactID `db:"id"`
	CurrentFlowID models.FlowID    `db:"current_flow_id"`
}

const sqlUpdateContactCurrentFlow = `
UPDATE 
	contacts_contact c
SET
	current_flow_id = r.current_flow_id::int
FROM (
	VALUES(:id, :current_flow_id)
) AS
	r(id, current_flow_id)
WHERE
	c.id = r.id::int
`
