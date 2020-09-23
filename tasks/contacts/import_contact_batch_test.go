package contacts_test

import (
	"testing"

	"github.com/nyaruka/mailroom"
	"github.com/nyaruka/mailroom/config"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/tasks/contacts"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"

	"github.com/stretchr/testify/require"
)

func TestImportContactBatch(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()

	mr := &mailroom.Mailroom{Config: config.Mailroom, DB: db, RP: testsuite.RP(), ElasticClient: nil}

	importID := testdata.InsertContactImport(t, db, models.Org1)
	batchID := testdata.InsertContactImportBatch(t, db, importID, `[{"name": "Bob"},{"name": "Jim"}]`)

	task := &contacts.ImportContactBatchTask{ContactImportBatchID: batchID}

	err := task.Perform(ctx, mr, models.Org1)
	require.NoError(t, err)
}
