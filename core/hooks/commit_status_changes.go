package hooks

import (
	"context"

	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

// CommitStatusChangesHook is our hook for status changes
var CommitStatusChangesHook models.EventCommitHook = &commitStatusChangesHook{}

type commitStatusChangesHook struct{}

// Apply commits our contact status change
func (h *commitStatusChangesHook) Apply(ctx context.Context, rt *runtime.Runtime, tx *sqlx.Tx, oa *models.OrgAssets, scenes map[*models.Scene][]interface{}) error {

	statusChanges := make([]*models.ContactStatusChange, 0, len(scenes))
	for scene, es := range scenes {

		event := es[len(es)-1].(*events.ContactStatusChangedEvent)
		statusChanges = append(statusChanges, &models.ContactStatusChange{ContactID: scene.ContactID(), Status: event.Status})
	}

	err := models.UpdateContactStatus(ctx, tx, statusChanges)
	if err != nil {
		return errors.Wrapf(err, "error updating contact statuses")
	}
	return nil
}
