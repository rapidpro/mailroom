package goflow_test

import (
	"encoding/json"
	"testing"

	"github.com/nyaruka/mailroom/core/goflow"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"

	"github.com/stretchr/testify/assert"
)

func TestReadModifiers(t *testing.T) {
	ctx, rt, _, _ := testsuite.Get()

	oa, err := models.GetOrgAssets(ctx, rt, testdata.Org1.ID)
	assert.NoError(t, err)

	// can read empty list
	mods, err := goflow.ReadModifiers(oa.SessionAssets(), []json.RawMessage{}, goflow.IgnoreMissing)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(mods))

	// can read non-empty list
	mods, err = goflow.ReadModifiers(oa.SessionAssets(), []json.RawMessage{
		[]byte(`{"type": "name", "name": "Bob"}`),
		[]byte(`{"type": "field", "field": {"key": "gender", "name": "Gender"}, "value": "M"}`),
		[]byte(`{"type": "language", "language": "spa"}`),
	}, goflow.IgnoreMissing)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(mods))
	assert.Equal(t, "name", mods[0].Type())
	assert.Equal(t, "field", mods[1].Type())
	assert.Equal(t, "language", mods[2].Type())

	// modifier with missing asset can be ignored
	mods, err = goflow.ReadModifiers(oa.SessionAssets(), []json.RawMessage{
		[]byte(`{"type": "name", "name": "Bob"}`),
		[]byte(`{"type": "field", "field": {"key": "blood_type", "name": "Blood Type"}, "value": "O"}`),
		[]byte(`{"type": "language", "language": "spa"}`),
	}, goflow.IgnoreMissing)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(mods))
	assert.Equal(t, "name", mods[0].Type())
	assert.Equal(t, "language", mods[1].Type())

	// modifier with missing asset or an error if allowMissing is false
	_, err = goflow.ReadModifiers(oa.SessionAssets(), []json.RawMessage{
		[]byte(`{"type": "name", "name": "Bob"}`),
		[]byte(`{"type": "field", "field": {"key": "blood_type", "name": "Blood Type"}, "value": "O"}`),
		[]byte(`{"type": "language", "language": "spa"}`),
	}, goflow.ErrorOnMissing)
	assert.EqualError(t, err, `error reading modifier: {"type": "field", "field": {"key": "blood_type", "name": "Blood Type"}, "value": "O"}: no modifier to return because of missing assets`)

	// error if any modifier structurally invalid
	_, err = goflow.ReadModifiers(oa.SessionAssets(), []json.RawMessage{
		[]byte(`{"type": "field", "value": "O"}`),
		[]byte(`{"type": "language", "language": "spa"}`),
	}, goflow.ErrorOnMissing)
	assert.EqualError(t, err, `error reading modifier: {"type": "field", "value": "O"}: field 'field' is required`)
}
