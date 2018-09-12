package models

import (
	"context"
	"testing"

	"github.com/nyaruka/goflow/flows"
	"github.com/stretchr/testify/assert"
)

func TestFields(t *testing.T) {
	ctx := context.Background()
	db := Reset(t)

	fields, err := loadFields(ctx, db, 1)
	assert.NoError(t, err)

	tcs := []struct {
		Key       string
		Name      string
		ValueType flows.FieldValueType
	}{
		{"age", "Age", flows.FieldValueTypeNumber},
		{"district", "District", flows.FieldValueTypeDistrict},
		{"gender", "Gender", flows.FieldValueTypeText},
		{"joined", "Joined On", flows.FieldValueTypeDatetime},
		{"state", "State", flows.FieldValueTypeState},
		{"ward", "Ward", flows.FieldValueTypeWard},
	}

	assert.Equal(t, 6, len(fields))
	for i, tc := range tcs {
		assert.Equal(t, tc.Key, fields[i].Key())
		assert.Equal(t, tc.Name, fields[i].Name())
		assert.Equal(t, tc.ValueType, fields[i].ValueType())
	}
}
