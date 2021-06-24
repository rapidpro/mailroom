package testdata

import (
	"encoding/json"

	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/mailroom/core/models"

	"github.com/jmoiron/sqlx"
)

// InsertContactImport inserts a contact import
func InsertContactImport(db *sqlx.DB, org *Org) models.ContactImportID {
	var importID models.ContactImportID
	must(db.Get(&importID, `INSERT INTO contacts_contactimport(org_id, file, original_filename, headers, mappings, num_records, group_id, started_on, created_on, created_by_id, modified_on, modified_by_id, is_active)
					          VALUES($1, 'contact_imports/1234.xlsx', 'contacts.xlsx', '{"Name", "URN:Tel"}', '{}', 30, NULL, $2, $2, 1, $2, 1, TRUE) RETURNING id`, org.ID, dates.Now(),
	))
	return importID
}

// InsertContactImportBatch inserts a contact import batch
func InsertContactImportBatch(db *sqlx.DB, importID models.ContactImportID, specs json.RawMessage) models.ContactImportBatchID {
	var splitSpecs []json.RawMessage
	must(jsonx.Unmarshal(specs, &splitSpecs))

	var batchID models.ContactImportBatchID
	must(db.Get(&batchID, `INSERT INTO contacts_contactimportbatch(contact_import_id, status, specs, record_start, record_end, num_created, num_updated, num_errored, errors, finished_on)
					         VALUES($1, 'P', $2, 0, $3, 0, 0, 0, '[]', NULL) RETURNING id`, importID, specs, len(splitSpecs),
	))
	return batchID
}
