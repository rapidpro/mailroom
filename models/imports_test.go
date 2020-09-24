package models_test

import (
	"testing"

	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	_ "github.com/nyaruka/mailroom/hooks"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestContactImportBatch(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()

	importID := testdata.InsertContactImport(t, db, models.Org1)
	batchID := testdata.InsertContactImportBatch(t, db, importID, `[
		{"name": "Norbert", "language": "eng", "urns": ["tel:+16055740001"]},
		{"name": "Leah", "urns": ["tel:+16055740002"]}
	]`)

	batch, err := models.LoadContactImportBatch(ctx, db, batchID)
	require.NoError(t, err)

	assert.Equal(t, importID, batch.ImportID)
	assert.Equal(t, models.ContactImportStatus("P"), batch.Status)
	assert.NotNil(t, batch.Specs)
	assert.Equal(t, 10, batch.RecordStart)
	assert.Equal(t, 12, batch.RecordEnd)

	err = batch.Import(ctx, db, models.Org1)
	require.NoError(t, err)

	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM contacts_contactimportbatch WHERE status = 'C' AND finished_on IS NOT NULL`, []interface{}{}, 1)
}

func TestContactSpecUnmarshal(t *testing.T) {
	s := &models.ContactSpec{}
	jsonx.Unmarshal([]byte(`{}`), s)

	assert.Equal(t, flows.ContactUUID(""), s.UUID)
	assert.Nil(t, s.Name)
	assert.Nil(t, s.Language)
	assert.Nil(t, s.URNs)
	assert.Nil(t, s.Fields)
	assert.Nil(t, s.Groups)

	s = &models.ContactSpec{}
	jsonx.Unmarshal([]byte(`{
		"uuid": "8e879527-7e6d-4bff-abc8-b1d41cd4f702", 
		"name": "Bob", 
		"language": "spa",
		"urns": ["tel:+1234567890"],
		"fields": {"age": "39"},
		"groups": ["3972dcc2-6749-4761-a896-7880d6165f2c"]
	}`), s)

	assert.Equal(t, flows.ContactUUID("8e879527-7e6d-4bff-abc8-b1d41cd4f702"), s.UUID)
	assert.Equal(t, "Bob", *s.Name)
	assert.Equal(t, "spa", *s.Language)
	assert.Equal(t, []urns.URN{"tel:+1234567890"}, s.URNs)
	assert.Equal(t, map[string]string{"age": "39"}, s.Fields)
	assert.Equal(t, []assets.GroupUUID{"3972dcc2-6749-4761-a896-7880d6165f2c"}, s.Groups)
}
