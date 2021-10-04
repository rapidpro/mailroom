package contacts

import (
	"context"
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/pkg/errors"
)

// TypeImportContactBatch is the type of the import contact batch task
const TypeImportContactBatch = "import_contact_batch"

func init() {
	tasks.RegisterType(TypeImportContactBatch, func() tasks.Task { return &ImportContactBatchTask{} })
}

// ImportContactBatchTask is our task to import a batch of contacts
type ImportContactBatchTask struct {
	ContactImportBatchID models.ContactImportBatchID `json:"contact_import_batch_id"`
}

// Timeout is the maximum amount of time the task can run for
func (t *ImportContactBatchTask) Timeout() time.Duration {
	return time.Minute * 10
}

// Perform figures out the membership for a query based group then repopulates it
func (t *ImportContactBatchTask) Perform(ctx context.Context, rt *runtime.Runtime, orgID models.OrgID) error {
	batch, err := models.LoadContactImportBatch(ctx, rt.DB, t.ContactImportBatchID)
	if err != nil {
		return errors.Wrapf(err, "unable to load contact import batch with id %d", t.ContactImportBatchID)
	}

	batchErr := batch.Import(ctx, rt, orgID)

	// decrement the redis key that holds remaining batches to see if the overall import is now finished
	rc := rt.RP.Get()
	defer rc.Close()
	remaining, _ := redis.Int(rc.Do("decr", fmt.Sprintf("contact_import_batches_remaining:%d", batch.ImportID)))
	if remaining == 0 {
		imp, err := models.LoadContactImport(ctx, rt.DB, batch.ImportID)
		if err != nil {
			return errors.Wrap(err, "error loading contact import")
		}

		// if any batch failed, then import is considered failed
		status := models.ContactImportStatusComplete
		for _, s := range imp.BatchStatuses {
			if models.ContactImportStatus(s) == models.ContactImportStatusFailed {
				status = models.ContactImportStatusFailed
				break
			}
		}

		if err := imp.MarkFinished(ctx, rt.DB, status); err != nil {
			return errors.Wrap(err, "error marking import as finished")
		}

		if err := models.NotifyImportFinished(ctx, rt.DB, imp); err != nil {
			return errors.Wrap(err, "error creating import finished notification")
		}
	}

	return errors.Wrapf(batchErr, "unable to import contact import batch %d", t.ContactImportBatchID)
}
