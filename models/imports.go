package models

import (
	"context"
	"encoding/json"
	"time"

	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/pkg/errors"

	"github.com/jmoiron/sqlx"
)

// ContactImportID is the type for contact import IDs
type ContactImportID int64

// ContactImportBatchID is the type for contact import batch IDs
type ContactImportBatchID int64

// ContactImportStatus is the status of an import
type ContactImportStatus string

// import status constants
const (
	ContactImportStatusPending    ContactImportStatus = "P"
	ContactImportStatusProcessing ContactImportStatus = "O"
	ContactImportStatusComplete   ContactImportStatus = "C"
	ContactImportStatusFailed     ContactImportStatus = "F"
)

// ContactImportBatch is a batch of contacts within a larger import
type ContactImportBatch struct {
	ID       ContactImportBatchID `db:"id"`
	ImportID ContactImportID      `db:"contact_import_id"`
	Status   ContactImportStatus  `db:"status"`
	Specs    json.RawMessage      `db:"specs"`

	// the range of records from the entire import contained in this batch
	RecordStart int `db:"record_start"`
	RecordEnd   int `db:"record_end"`

	// results written after processing this batch
	NumCreated int             `db:"num_created"`
	NumUpdated int             `db:"num_updated"`
	NumErrored int             `db:"num_errored"`
	Errors     json.RawMessage `db:"errors"`
	FinishedOn *time.Time      `db:"finished_on"`
}

// Import does the actual import of this batch
func (b *ContactImportBatch) Import(ctx context.Context, db *sqlx.DB) error {
	// if any error occurs this batch should be marked as failed
	if err := b.tryImport(ctx, db); err != nil {
		b.markFailed(ctx, db)
		return err
	}
	return nil
}

func (b *ContactImportBatch) tryImport(ctx context.Context, db *sqlx.DB) error {
	if err := b.markProcessing(ctx, db); err != nil {
		return errors.Wrap(err, "unable to mark as processing")
	}

	// unmarshal this batch's specs
	var specs []*ContactSpec
	if err := jsonx.Unmarshal(b.Specs, &specs); err != nil {
		return errors.New("unable to unmarsal specs")
	}

	result, err := importContactSpecs(ctx, db, specs)
	if err != nil {
		return errors.Wrap(err, "unable to import specs")
	}

	if err := b.markComplete(ctx, db, result); err != nil {
		return errors.Wrap(err, "unable to mark as complete")
	}

	return nil
}

func (b *ContactImportBatch) markProcessing(ctx context.Context, db *sqlx.DB) error {
	b.Status = ContactImportStatusProcessing
	_, err := db.ExecContext(ctx, `UPDATE contacts_contactimportbatch SET status = $2 WHERE id = $1`, b.ID, b.Status)
	return err
}

func (b *ContactImportBatch) markComplete(ctx context.Context, db *sqlx.DB, r importResult) error {
	errorsJSON, err := jsonx.Marshal(r.errors)
	if err != nil {
		return errors.Wrap(err, "error marshaling errors")
	}

	now := dates.Now()
	b.Status = ContactImportStatusComplete
	b.NumCreated = r.numCreated
	b.NumUpdated = r.numUpdated
	b.NumErrored = r.numErrored
	b.Errors = errorsJSON
	b.FinishedOn = &now
	_, err = db.ExecContext(ctx,
		`UPDATE contacts_contactimportbatch SET status = $2, num_created = $3, num_updated = $4, num_errored = $5, errors = $6, finished_on = $7 WHERE id = $1`,
		b.ID, b.Status, b.NumCreated, b.NumUpdated, b.NumErrored, b.Errors, b.FinishedOn,
	)
	return err
}

func (b *ContactImportBatch) markFailed(ctx context.Context, db *sqlx.DB) error {
	now := dates.Now()
	b.Status = ContactImportStatusFailed
	b.FinishedOn = &now
	_, err := db.ExecContext(ctx, `UPDATE contacts_contactimportbatch SET status = $2, finished_on = $3 WHERE id = $1`, b.ID, b.Status, b.FinishedOn)
	return err
}

var loadContactImportBatchSQL = `
SELECT 
	id,
  	contact_import_id,
  	status,
  	specs,
  	record_start,
  	record_end
FROM
	contacts_contactimportbatch
WHERE
	id = $1`

// LoadContactImportBatch loads a contact import batch by ID
func LoadContactImportBatch(ctx context.Context, db Queryer, id ContactImportBatchID) (*ContactImportBatch, error) {
	b := &ContactImportBatch{}
	err := db.GetContext(ctx, b, loadContactImportBatchSQL, id)
	if err != nil {
		return nil, err
	}
	return b, nil
}

// ContactSpec describes a contact to be updated or created
type ContactSpec struct {
	UUID     flows.ContactUUID  `json:"uuid"`
	Name     *string            `json:"name"`
	Language *string            `json:"language"`
	URNs     []urns.URN         `json:"urns"`
	Fields   map[string]string  `json:"fields"`
	Groups   []assets.GroupUUID `json:"groups"`
}

// an error message associated with a particular record
type importError struct {
	Record  int    `json:"record"`
	Message string `json:"message"`
}

// holds the result of importing a set of contact specs
type importResult struct {
	numCreated int
	numUpdated int
	numErrored int
	errors     []importError
}

func importContactSpecs(ctx context.Context, db *sqlx.DB, specs []*ContactSpec) (importResult, error) {
	res := importResult{
		errors: make([]importError, 0),
	}

	// TODO

	return res, nil
}
