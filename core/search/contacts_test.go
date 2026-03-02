package search_test

import (
	"sort"
	"testing"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/search"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewContactDoc(t *testing.T) {
	ctx, rt := testsuite.Runtime(t)

	oa := testdb.Org1.Load(t, rt)

	mcs, err := models.LoadContacts(ctx, rt.DB, oa, []models.ContactID{testdb.Ann.ID, testdb.Cat.ID})
	require.NoError(t, err)
	require.Len(t, mcs, 2)

	sort.Slice(mcs, func(i, j int) bool { return mcs[i].ID() < mcs[j].ID() })

	// convert to flow contacts
	flowContacts := make(map[models.ContactID]*flows.Contact)
	for _, mc := range mcs {
		fc, err := mc.EngineContact(oa)
		require.NoError(t, err)
		flowContacts[mc.ID()] = fc
	}

	// Ann: has name, status=active, URNs, groups, fields (gender, state, district, ward)
	annFC := flowContacts[testdb.Ann.ID]
	require.NotNil(t, annFC)

	doc := search.NewContactDoc(oa, annFC)

	assert.Equal(t, testdb.Ann.ID, doc.LegacyID)
	assert.Equal(t, testdb.Org1.ID, doc.OrgID)
	assert.Equal(t, testdb.Ann.UUID, doc.UUID)
	assert.Equal(t, "Ann", doc.Name)
	assert.Equal(t, models.ContactStatusActive, doc.Status)
	assert.NotEmpty(t, doc.CreatedOn)

	// Ann should have URNs
	assert.Len(t, doc.URNs, 1)
	assert.Equal(t, "tel", doc.URNs[0].Scheme)
	assert.Equal(t, "+16055741111", doc.URNs[0].Path)

	// Ann should be in the Doctors group
	assert.Contains(t, doc.GroupIDs, testdb.DoctorsGroup.ID)

	// Ann has no open tickets by default in test fixtures
	assert.Equal(t, 0, doc.Tickets)

	// Ann should have fields: gender, state, district, ward (not age since it's nil)
	fieldsByUUID := make(map[assets.FieldUUID]*search.ContactFieldDoc)
	for _, f := range doc.Fields {
		fieldsByUUID[f.Field] = f
	}

	genderField := fieldsByUUID[testdb.GenderField.UUID]
	require.NotNil(t, genderField, "should have gender field")
	assert.Equal(t, "F", genderField.Text)

	stateField := fieldsByUUID[testdb.StateField.UUID]
	require.NotNil(t, stateField, "should have state field")
	assert.NotEmpty(t, stateField.State)
	assert.NotEmpty(t, stateField.StateKeyword)

	wardField := fieldsByUUID[testdb.WardField.UUID]
	require.NotNil(t, wardField, "should have ward field")
	assert.NotEmpty(t, wardField.Ward)
	assert.NotEmpty(t, wardField.WardKeyword)

	// Cat: has name, status=active, age=30, 1 URN, in Doctors group, no tickets
	catFC := flowContacts[testdb.Cat.ID]
	require.NotNil(t, catFC)

	doc = search.NewContactDoc(oa, catFC)

	assert.Equal(t, testdb.Cat.ID, doc.LegacyID)
	assert.Equal(t, testdb.Cat.UUID, doc.UUID)
	assert.Equal(t, "Cat", doc.Name)
	assert.Equal(t, models.ContactStatusActive, doc.Status)

	assert.Len(t, doc.URNs, 1)
	assert.Equal(t, "tel", doc.URNs[0].Scheme)

	assert.Equal(t, 0, doc.Tickets)

	// Cat should have age field with number
	fieldsByUUID = make(map[assets.FieldUUID]*search.ContactFieldDoc)
	for _, f := range doc.Fields {
		fieldsByUUID[f.Field] = f
	}

	ageField := fieldsByUUID[testdb.AgeField.UUID]
	require.NotNil(t, ageField, "should have age field")
	assert.NotNil(t, ageField.Number)
}
