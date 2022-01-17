package hooks

import (
	"context"

	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

// InsertHTTPLogsHook is our hook for inserting classifier logs
var InsertHTTPLogsHook models.EventCommitHook = &insertHTTPLogsHook{}

type insertHTTPLogsHook struct{}

// Apply inserts all the classifier logs that were created
func (h *insertHTTPLogsHook) Apply(ctx context.Context, rt *runtime.Runtime, tx *sqlx.Tx, oa *models.OrgAssets, scenes map[*models.Scene][]interface{}) error {
	// gather all our logs
	logs := make([]*models.HTTPLog, 0, len(scenes))
	for _, ls := range scenes {
		for _, l := range ls {
			logs = append(logs, l.(*models.HTTPLog))
		}
	}

	err := models.InsertHTTPLogs(ctx, tx, logs)
	if err != nil {
		return errors.Wrapf(err, "error inserting http logs")
	}

	return nil
}
