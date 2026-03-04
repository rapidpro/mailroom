package contact

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/web"
)

func init() {
	web.InternalRoute(http.MethodPost, "/contact/import", web.JSONPayload(handleImport))
}

// Request that a contact import is started.
//
//	{
//	  "org_id": 1,
//	  "import_id": 123
//	}
type importRequest struct {
	OrgID    models.OrgID           `json:"org_id"    validate:"required"`
	ImportID models.ContactImportID `json:"import_id" validate:"required"`
}

func handleImport(ctx context.Context, rt *runtime.Runtime, r *importRequest) (any, int, error) {
	imp, err := models.LoadContactImport(ctx, rt.DB, r.ImportID)
	if err != nil {
		return nil, 0, err
	}
	if imp.OrgID != r.OrgID {
		panic("request org id does not match import org id")
	}
	if imp.Status != models.ImportStatusProcessing {
		return nil, 0, fmt.Errorf("import is not processing")
	}

	// set valkey counter which batch tasks can decrement to know when import has completed
	counter := tasks.NewCounter(fmt.Sprintf("contact_import_batches_remaining:%d", imp.ID), 24*time.Hour)
	if err := counter.Init(ctx, rt.VK, len(imp.BatchIDs)); err != nil {
		return nil, 0, fmt.Errorf("error setting import batch counter key: %w", err)
	}

	// create tasks for all batches
	for _, bID := range imp.BatchIDs {
		task := &tasks.ImportContactBatch{ContactImportBatchID: bID}
		if err := tasks.Queue(ctx, rt, rt.Queues.Batch, r.OrgID, task, false); err != nil {
			return nil, 0, fmt.Errorf("error queuing import contact batch task: %w", err)
		}
	}

	return map[string]any{"batches": len(imp.BatchIDs)}, http.StatusOK, nil
}
