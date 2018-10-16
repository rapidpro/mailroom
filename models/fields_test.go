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

	fields, err := loadFields(ctx, db, 1)
	assert.NoError(t, err)

	tcs := []struct {
		Key       string
		Name      string
		ValueType assets.FieldType
	}{
		{"age", "Age", assets.FieldTypeNumber},
		{"district", "District", assets.FieldTypeDistrict},
		{"gender", "Gender", assets.FieldTypeText},
		{"joined", "Joined On", assets.FieldTypeDatetime},
		{"state", "State", assets.FieldTypeState},
		{"ward", "Ward", assets.FieldTypeWard},
	}

	assert.Equal(t, 6, len(fields))
	for i, tc := range tcs {
		assert.Equal(t, tc.Key, fields[i].Key())
		assert.Equal(t, tc.Name, fields[i].Name())
		assert.Equal(t, tc.ValueType, fields[i].Type())
	}
}
