package models

import (
	"testing"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/stretchr/testify/assert"
)

func TestFields(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()

	userFields, systemFields, err := loadFields(ctx, db, 1)
	assert.NoError(t, err)

	expectedUserFields := []struct {
		Key       string
		Name      string
		ValueType assets.FieldType
	}{
		{"age", "Age", assets.FieldTypeNumber},
		{"district", "District", assets.FieldTypeDistrict},
		{"gender", "Gender", assets.FieldTypeText},
		{"joined", "Joined", assets.FieldTypeDatetime},
		{"state", "State", assets.FieldTypeState},
		{"ward", "Ward", assets.FieldTypeWard},
	}

	assert.Equal(t, len(expectedUserFields), len(userFields))
	for i, tc := range expectedUserFields {
		assert.Equal(t, tc.Key, userFields[i].Key())
		assert.Equal(t, tc.Name, userFields[i].Name())
		assert.Equal(t, tc.ValueType, userFields[i].Type())
	}

	expectedSystemFields := []struct {
		Key       string
		Name      string
		ValueType assets.FieldType
	}{
		{"created_on", "Created On", assets.FieldTypeDatetime},
		{"id", "ID", assets.FieldTypeNumber},
		{"language", "Language", assets.FieldTypeText},
		{"last_seen_on", "Last Seen On", assets.FieldTypeDatetime},
		{"name", "Name", assets.FieldTypeText},
	}

	assert.Equal(t, len(expectedSystemFields), len(systemFields))
	for i, tc := range expectedSystemFields {
		assert.Equal(t, tc.Key, systemFields[i].Key())
		assert.Equal(t, tc.Name, systemFields[i].Name())
		assert.Equal(t, tc.ValueType, systemFields[i].Type())
	}
}
