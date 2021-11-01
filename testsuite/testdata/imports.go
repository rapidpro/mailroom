package testdata

import (
	"encoding/json"
	"testing"

	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/mailroom/core/models"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

// InsertContactImport inserts a contact import
func InsertContactImport(t *testing.T, db *sqlx.DB, orgID models.OrgID, validateCarrier bool) models.ContactImportID {
	var importID models.ContactImportID
	err := db.Get(&importID, `INSERT INTO contacts_contactimport(org_id, file, original_filename, headers, mappings, num_records, group_id, started_on, created_on, created_by_id, modified_on, modified_by_id, is_active, num_duplicates, validate_carrier)
					          VALUES($1, 'contact_imports/1234.xlsx', 'contacts.xlsx', '{"Name", "URN:Tel"}', '{}', 30, NULL, $2, $2, 1, $2, 1, TRUE, 0, $3) RETURNING id`, models.Org1, dates.Now(), validateCarrier)
	require.NoError(t, err)
	return importID
}

// InsertContactImportBatch inserts a contact import batch
func InsertContactImportBatch(t *testing.T, db *sqlx.DB, importID models.ContactImportID, specs json.RawMessage) models.ContactImportBatchID {
	var splitSpecs []json.RawMessage
	err := jsonx.Unmarshal(specs, &splitSpecs)
	require.NoError(t, err)

	var batchID models.ContactImportBatchID
	err = db.Get(&batchID, `INSERT INTO contacts_contactimportbatch(contact_import_id, status, specs, record_start, record_end, num_created, num_updated, num_errored, errors, finished_on, num_blocked, blocked_uuids, carrier_groups)
					         VALUES($1, 'P', $2, 0, $3, 0, 0, 0, '[]', NULL, 0, '[]', '{}') RETURNING id`, importID, specs, len(splitSpecs))
	require.NoError(t, err)
	return batchID
}
