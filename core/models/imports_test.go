package models_test

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"sort"
	"strings"
	"testing"

	"github.com/greatnonprofits-nfp/goflow/assets"
	"github.com/greatnonprofits-nfp/goflow/excellent/types"
	"github.com/greatnonprofits-nfp/goflow/flows"
	"github.com/greatnonprofits-nfp/goflow/test"
	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/gocommon/uuids"
	_ "github.com/nyaruka/mailroom/core/handlers"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestContactImports(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()
	testsuite.Reset()
	defer testsuite.Reset()

	models.FlushCache()

	testdata.DeleteContactsAndURNs(t, db)

	// add contact in other org to make sure we can't update it
	testdata.InsertContact(t, db, models.Org2, "f7a8016d-69a6-434b-aae7-5142ce4a98ba", "Xavier", "spa")

	// add dynamic group to test imported contacts are added to it
	testdata.InsertContactGroup(t, db, models.Org1, "fc32f928-ad37-477c-a88e-003d30fd7406", "Adults", "age >= 40")

	// give our org a country by setting country on a channel
	db.MustExec(`UPDATE channels_channel SET country = 'US' WHERE id = $1`, models.TwilioChannelID)

	testJSON, err := ioutil.ReadFile("testdata/imports.json")
	require.NoError(t, err)

	tcs := []struct {
		Description string                `json:"description"`
		Specs       json.RawMessage       `json:"specs"`
		NumCreated  int                   `json:"num_created"`
		NumUpdated  int                   `json:"num_updated"`
		NumErrored  int                   `json:"num_errored"`
		Errors      json.RawMessage       `json:"errors"`
		Contacts    []*models.ContactSpec `json:"contacts"`
	}{}
	err = jsonx.Unmarshal(testJSON, &tcs)
	require.NoError(t, err)

	oa, err := models.GetOrgAssets(ctx, db, 1)
	require.NoError(t, err)

	uuids.SetGenerator(uuids.NewSeededGenerator(12345))
	defer uuids.SetGenerator(uuids.DefaultGenerator)

	for i, tc := range tcs {
		importID := testdata.InsertContactImport(t, db, models.Org1, false)
		batchID := testdata.InsertContactImportBatch(t, db, importID, tc.Specs)

		batch, err := models.LoadContactImportBatch(ctx, db, batchID)
		require.NoError(t, err)

		err = batch.Import(ctx, db, models.Org1)
		require.NoError(t, err)

		results := &struct {
			NumCreated int             `db:"num_created"`
			NumUpdated int             `db:"num_updated"`
			NumErrored int             `db:"num_errored"`
			Errors     json.RawMessage `db:"errors"`
		}{}
		err = db.Get(results, `SELECT num_created, num_updated, num_errored, errors FROM contacts_contactimportbatch WHERE id = $1`, batchID)
		require.NoError(t, err)

		// load all contacts and convert to specs
		contacts := loadAllContacts(t, db, oa)
		specs := make([]*models.ContactSpec, len(contacts))
		for i, contact := range contacts {
			name := contact.Name()
			lang := string(contact.Language())
			groupUUIDs := make([]assets.GroupUUID, len(contact.Groups().All()))
			for j, group := range contact.Groups().All() {
				groupUUIDs[j] = group.UUID()
			}
			sort.Slice(groupUUIDs, func(i, j int) bool { return strings.Compare(string(groupUUIDs[i]), string(groupUUIDs[j])) < 0 })

			fields := make(map[string]string)
			for key, fv := range contact.Fields() {
				val := types.Render(fv.ToXValue(oa.Env()))
				if val != "" {
					fields[key] = val
				}
			}
			specs[i] = &models.ContactSpec{
				UUID:     contact.UUID(),
				Name:     &name,
				Language: &lang,
				URNs:     contact.URNs().RawURNs(),
				Fields:   fields,
				Groups:   groupUUIDs,
			}
		}

		actual := tc
		actual.NumCreated = results.NumCreated
		actual.NumUpdated = results.NumUpdated
		actual.NumErrored = results.NumErrored
		actual.Errors = results.Errors
		actual.Contacts = specs

		if !test.UpdateSnapshots {
			assert.Equal(t, tc.NumCreated, actual.NumCreated, "created contacts mismatch in '%s'", tc.Description)
			assert.Equal(t, tc.NumUpdated, actual.NumUpdated, "updated contacts mismatch in '%s'", tc.Description)
			assert.Equal(t, tc.NumErrored, actual.NumErrored, "errored contacts mismatch in '%s'", tc.Description)

			test.AssertEqualJSON(t, tc.Errors, actual.Errors, "errors mismatch in '%s'", tc.Description)

			actualJSON, _ := jsonx.Marshal(actual.Contacts)
			expectedJSON, _ := jsonx.Marshal(tc.Contacts)
			test.AssertEqualJSON(t, expectedJSON, actualJSON, "imported contacts mismatch in '%s'", tc.Description)
		} else {
			tcs[i] = actual
		}
	}

	if test.UpdateSnapshots {
		testJSON, err = jsonx.MarshalPretty(tcs)
		require.NoError(t, err)

		err = ioutil.WriteFile("testdata/imports.json", testJSON, 0600)
		require.NoError(t, err)
	}
}

func TestContactImportBatch(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()

	importID := testdata.InsertContactImport(t, db, models.Org1, false)
	batchID := testdata.InsertContactImportBatch(t, db, importID, []byte(`[
		{"name": "Norbert", "language": "eng", "urns": ["tel:+16055740001"]},
		{"name": "Leah", "urns": ["tel:+16055740002"]}
	]`))

	batch, err := models.LoadContactImportBatch(ctx, db, batchID)
	require.NoError(t, err)

	assert.Equal(t, importID, batch.ImportID)
	assert.Equal(t, models.ContactImportStatus("P"), batch.Status)
	assert.NotNil(t, batch.Specs)
	assert.Equal(t, 0, batch.RecordStart)
	assert.Equal(t, 2, batch.RecordEnd)

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

func TestImportWithCarrierValidation(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()

	importID := testdata.InsertContactImport(t, db, models.Org1, true)
	batchID := testdata.InsertContactImportBatch(t, db, importID, []byte(`[
		{"name": "Norbert", "language": "eng", "urns": ["tel:+16055740001"]},
		{"name": "Leah", "urns": ["tel:+16055740002"]}
	]`))

	batch, err := models.LoadContactImportBatch(ctx, db, batchID)
	require.NoError(t, err)

	err = batch.Import(ctx, db, models.Org1)
	require.NoError(t, err)

	carrierGroups := map[models.CarrierType][]models.ContactID{}
	jsonx.Unmarshal(batch.CarrierGroups, &carrierGroups)
	assert.Equal(t, len(carrierGroups), 1)
	assert.Equal(t, len(carrierGroups["mobile"]), 2)
}

// utility to load all contacts for the given org and return as slice sorted by ID
func loadAllContacts(t *testing.T, db *sqlx.DB, oa *models.OrgAssets) []*flows.Contact {
	rows, err := db.Queryx(`SELECT id FROM contacts_contact WHERE org_id = $1`, oa.OrgID())
	require.NoError(t, err)
	defer rows.Close()

	var allIDs []models.ContactID
	var id models.ContactID
	for rows.Next() {
		rows.Scan(&id)
		allIDs = append(allIDs, id)
	}

	contacts, err := models.LoadContacts(context.Background(), db, oa, allIDs)
	require.NoError(t, err)

	sort.Slice(contacts, func(i, j int) bool { return contacts[i].ID() < contacts[j].ID() })

	flowContacts := make([]*flows.Contact, len(contacts))
	for i := range contacts {
		flowContacts[i], err = contacts[i].FlowContact(oa)
		require.NoError(t, err)
	}

	return flowContacts
}
