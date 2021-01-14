package hooks

import (
	"context"

	"github.com/nyaruka/mailroom/core/models"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

// InsertAirtimeTransfersHook is our hook for inserting airtime transfers
var InsertAirtimeTransfersHook models.EventCommitHook = &insertAirtimeTransfersHook{}

type insertAirtimeTransfersHook struct{}

// Apply inserts all the airtime transfers that were created
func (h *insertAirtimeTransfersHook) Apply(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, oa *models.OrgAssets, scenes map[*models.Scene][]interface{}) error {
	// gather all our transfers
	transfers := make([]*models.AirtimeTransfer, 0, len(scenes))

	for _, ts := range scenes {
		for _, t := range ts {
			transfer := t.(*models.AirtimeTransfer)
			transfers = append(transfers, transfer)
		}
	}

	// insert the transfers
	err := models.InsertAirtimeTransfers(ctx, tx, transfers)
	if err != nil {
		return errors.Wrapf(err, "error inserting airtime transfers")
	}

	// gather all our logs and set the newly inserted transfer IDs on them
	logs := make([]*models.HTTPLog, 0, len(scenes))

	for _, t := range transfers {
		for _, l := range t.Logs {
			l.SetAirtimeTransferID(t.ID())
			logs = append(logs, l)
		}
	}

	// insert the logs
	err = models.InsertHTTPLogs(ctx, tx, logs)
	if err != nil {
		return errors.Wrapf(err, "error inserting airtime transfer logs")
	}

	return nil
}
