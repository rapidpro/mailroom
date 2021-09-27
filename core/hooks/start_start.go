package hooks

import (
	"context"

	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/queue"
	"github.com/nyaruka/mailroom/runtime"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

// StartStartHook is our hook to fire our scene starts
var StartStartHook models.EventCommitHook = &startStartHook{}

type startStartHook struct{}

// Apply queues up our flow starts
func (h *startStartHook) Apply(ctx context.Context, rt *runtime.Runtime, tx *sqlx.Tx, oa *models.OrgAssets, scenes map[*models.Scene][]interface{}) error {
	rc := rt.RP.Get()
	defer rc.Close()

	// for each of our scene
	for _, es := range scenes {
		for _, e := range es {
			start := e.(*models.FlowStart)

			taskQ := queue.HandlerQueue
			priority := queue.DefaultPriority

			// if we are starting groups, queue to our batch queue instead, but with high priority
			if len(start.GroupIDs()) > 0 || start.Query() != "" {
				taskQ = queue.BatchQueue
				priority = queue.HighPriority
			}

			err := queue.AddTask(rc, taskQ, queue.StartFlow, int(oa.OrgID()), start, priority)
			if err != nil {
				return errors.Wrapf(err, "error queuing flow start")
			}
		}
	}

	return nil
}
