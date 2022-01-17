package models_test

import (
	"testing"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFields(t *testing.T) {
	ctx, rt, _, _ := testsuite.Get()

	oa, err := models.GetOrgAssetsWithRefresh(ctx, rt, testdata.Org1.ID, models.RefreshFields)
	require.NoError(t, err)

	expectedFields := []struct {
		field     testdata.Field
		key       string
		name      string
		valueType assets.FieldType
	}{
		{*testdata.GenderField, "gender", "Gender", assets.FieldTypeText},
		{*testdata.AgeField, "age", "Age", assets.FieldTypeNumber},
		{*testdata.CreatedOnField, "created_on", "Created On", assets.FieldTypeDatetime},
		{*testdata.LastSeenOnField, "last_seen_on", "Last Seen On", assets.FieldTypeDatetime},
	}
	for _, tc := range expectedFields {
		field := oa.FieldByUUID(tc.field.UUID)
		require.NotNil(t, field, "no such field: %s", tc.field.UUID)

		fieldByKey := oa.FieldByKey(tc.key)
		assert.Equal(t, field, fieldByKey)

		assert.Equal(t, tc.field.UUID, field.UUID(), "uuid mismatch for field %s", tc.field.ID)
		assert.Equal(t, tc.key, field.Key())
		assert.Equal(t, tc.name, field.Name())
		assert.Equal(t, tc.valueType, field.Type())
	}
}
