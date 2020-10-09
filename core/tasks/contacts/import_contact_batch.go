package contacts

import (
	"context"
	"time"

	"github.com/nyaruka/mailroom"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/tasks"
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
func (t *ImportContactBatchTask) Perform(ctx context.Context, mr *mailroom.Mailroom, orgID models.OrgID) error {
	batch, err := models.LoadContactImportBatch(ctx, mr.DB, t.ContactImportBatchID)
	if err != nil {
		return errors.Wrapf(err, "unable to load contact import batch with id %d", t.ContactImportBatchID)
	}

	if err := batch.Import(ctx, mr.DB, orgID); err != nil {
		return errors.Wrapf(err, "unable to import contact import batch %d", t.ContactImportBatchID)
	}

	return nil
}
