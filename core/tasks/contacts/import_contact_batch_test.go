package contacts_test

import (
	"fmt"
	"testing"

	"github.com/nyaruka/gocommon/dbutil/assertdb"
	_ "github.com/nyaruka/mailroom/core/handlers"
	"github.com/nyaruka/mailroom/core/tasks/contacts"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"

	"github.com/stretchr/testify/require"
)

func TestImportContactBatch(t *testing.T) {
	ctx, rt, db, rp := testsuite.Get()
	rc := rp.Get()
	defer rc.Close()

	defer testsuite.Reset(testsuite.ResetData)

	importID := testdata.InsertContactImport(db, testdata.Org1, testdata.Admin)
	batch1ID := testdata.InsertContactImportBatch(db, importID, []byte(`[
		{"name": "Norbert", "language": "eng", "urns": ["tel:+16055740001"]},
		{"name": "Leah", "urns": ["tel:+16055740002"]}
	]`))
	batch2ID := testdata.InsertContactImportBatch(db, importID, []byte(`[
		{"name": "Rowan", "language": "spa", "urns": ["tel:+16055740003"]}
	]`))

	rc.Do("setex", fmt.Sprintf("contact_import_batches_remaining:%d", importID), 10, 2)

	// perform first batch task...
	task1 := &contacts.ImportContactBatchTask{ContactImportBatchID: batch1ID}
	err := task1.Perform(ctx, rt, testdata.Org1.ID)
	require.NoError(t, err)

	// import is still in progress
	assertdb.Query(t, db, `SELECT status FROM contacts_contactimport WHERE id = $1`, importID).Columns(map[string]interface{}{"status": "O"})

	// perform second batch task...
	task2 := &contacts.ImportContactBatchTask{ContactImportBatchID: batch2ID}
	err = task2.Perform(ctx, rt, testdata.Org1.ID)
	require.NoError(t, err)

	assertdb.Query(t, db, `SELECT count(*) FROM contacts_contact WHERE id >= 30000`).Returns(3)
	assertdb.Query(t, db, `SELECT count(*) FROM contacts_contact WHERE name = 'Norbert' AND language = 'eng'`).Returns(1)
	assertdb.Query(t, db, `SELECT count(*) FROM contacts_contact WHERE name = 'Leah' AND language IS NULL`).Returns(1)
	assertdb.Query(t, db, `SELECT count(*) FROM contacts_contact WHERE name = 'Rowan' AND language = 'spa'`).Returns(1)

	// import is now complete and there is a notification for the creator
	assertdb.Query(t, db, `SELECT status FROM contacts_contactimport WHERE id = $1`, importID).Columns(map[string]interface{}{"status": "C"})
	assertdb.Query(t, db, `SELECT org_id, notification_type, scope, user_id FROM notifications_notification WHERE contact_import_id = $1`, importID).
		Columns(map[string]interface{}{
			"org_id":            int64(testdata.Org1.ID),
			"notification_type": "import:finished",
			"scope":             fmt.Sprintf("contact:%d", importID),
			"user_id":           int64(testdata.Admin.ID),
		})
}
