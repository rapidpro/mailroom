package tasks

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/nyaruka/mailroom/core/imports"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
)

// TypeImportContactBatch is the type of the import contact batch task
const TypeImportContactBatch = "import_contact_batch"

func init() {
	RegisterType(TypeImportContactBatch, func() Task { return &ImportContactBatch{} })
}

// ImportContactBatch is our task to import a batch of contacts
type ImportContactBatch struct {
	ContactImportBatchID models.ContactImportBatchID `json:"contact_import_batch_id"`
}

func (t *ImportContactBatch) Type() string {
	return TypeImportContactBatch
}

// Timeout is the maximum amount of time the task can run for
func (t *ImportContactBatch) Timeout() time.Duration {
	return time.Minute * 10
}

func (t *ImportContactBatch) WithAssets() models.Refresh {
	return models.RefreshFields | models.RefreshGroups
}

// Perform figures out the membership for a query based group then repopulates it
func (t *ImportContactBatch) Perform(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets) error {
	batch, err := models.LoadContactImportBatch(ctx, rt.DB, t.ContactImportBatchID)
	if err != nil {
		return fmt.Errorf("error loading contact import batch: %w", err)
	}

	imp, err := models.LoadContactImport(ctx, rt.DB, batch.ImportID)
	if err != nil {
		return fmt.Errorf("error loading contact import: %w", err)
	}

	batchErr := imports.ImportBatch(ctx, rt, oa, batch, imp.CreatedByID)

	// if any error occurs this batch should be marked as failed
	if batchErr != nil {
		batch.SetFailed(ctx, rt.DB)
	}

	// decrement the counter to see if the overall import is now finished
	counter := NewCounter(fmt.Sprintf("contact_import_batches_remaining:%d", batch.ImportID), 24*time.Hour)
	done, err := counter.Done(ctx, rt.VK)
	if err != nil {
		return fmt.Errorf("error decrementing import batch counter: %w", err)
	}
	if done {
		// if any batch failed, then import is considered failed
		success := !slices.Contains(imp.BatchStatuses, models.ImportStatusFailed)

		if err := imp.SetFinished(ctx, rt.DB, success); err != nil {
			return fmt.Errorf("error marking import as finished: %w", err)
		}

		if err := models.NotifyImportFinished(ctx, rt.DB, imp); err != nil {
			return fmt.Errorf("error creating import finished notification: %w", err)
		}
	}

	if batchErr != nil {
		return fmt.Errorf("unable to import contact import batch %d: %w", t.ContactImportBatchID, batchErr)
	}

	return nil
}
