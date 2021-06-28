package contacts_test

import (
	"testing"

	"github.com/nyaruka/mailroom"
	"github.com/nyaruka/mailroom/config"
	_ "github.com/nyaruka/mailroom/core/handlers"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/tasks/contacts"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"

	"github.com/stretchr/testify/require"
)

func TestImportContactBatch(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()

	mr := &mailroom.Mailroom{Config: config.Mailroom, DB: db, RP: testsuite.RP(), ElasticClient: nil}

	importID := testdata.InsertContactImport(t, db, models.Org1)
	batchID := testdata.InsertContactImportBatch(t, db, importID, []byte(`[
		{"name": "Norbert", "language": "eng", "urns": ["tel:+16055740001"]},
		{"name": "Leah", "urns": ["tel:+16055740002"]}
	]`))

	task := &contacts.ImportContactBatchTask{ContactImportBatchID: batchID}

	err := task.Perform(ctx, mr, models.Org1)
	require.NoError(t, err)

	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM contacts_contact WHERE name = 'Norbert' AND language = 'eng'`, nil, 1)
	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM contacts_contact WHERE name = 'Leah' AND language IS NULL`, nil, 1)
}
