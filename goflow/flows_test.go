package goflow_test

import (
	"testing"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/test"
	"github.com/nyaruka/goflow/utils/uuids"
	"github.com/nyaruka/mailroom/goflow"

	"github.com/Masterminds/semver"
	"github.com/stretchr/testify/assert"
)

func TestSpecVersion(t *testing.T) {
	assert.Equal(t, semver.MustParse("13.1.0"), goflow.SpecVersion())
}

func TestReadFlow(t *testing.T) {
	// try to read empty definition
	flow, err := goflow.ReadFlow([]byte(`{}`))
	assert.Nil(t, flow)
	assert.EqualError(t, err, "unable to read flow header: field 'uuid' is required, field 'spec_version' is required")

	// read legacy definition
	flow, err = goflow.ReadFlow([]byte(`{"flow_type": "M", "base_language": "eng", "action_sets": [], "metadata": {"uuid": "502c3ee4-3249-4dee-8e71-c62070667d52", "name": "Legacy"}}`))
	assert.Nil(t, err)
	assert.Equal(t, assets.FlowUUID("502c3ee4-3249-4dee-8e71-c62070667d52"), flow.UUID())
	assert.Equal(t, "Legacy", flow.Name())
	assert.Equal(t, envs.Language("eng"), flow.Language())
	assert.Equal(t, flows.FlowTypeMessaging, flow.Type())

	// read new definition
	flow, err = goflow.ReadFlow([]byte(`{"uuid": "502c3ee4-3249-4dee-8e71-c62070667d52", "name": "New", "spec_version": "13.0.0", "type": "messaging", "language": "eng", "nodes": []}`))
	assert.Nil(t, err)
	assert.Equal(t, assets.FlowUUID("502c3ee4-3249-4dee-8e71-c62070667d52"), flow.UUID())
	assert.Equal(t, "New", flow.Name())
	assert.Equal(t, envs.Language("eng"), flow.Language())
}

func TestCloneDefinition(t *testing.T) {
	uuids.SetGenerator(uuids.NewSeededGenerator(12345))
	defer uuids.SetGenerator(uuids.DefaultGenerator)

	cloned, err := goflow.CloneDefinition([]byte(`{"uuid": "502c3ee4-3249-4dee-8e71-c62070667d52", "name": "New", "spec_version": "13.0.0", "type": "messaging", "language": "eng", "nodes": []}`), nil)
	assert.NoError(t, err)
	test.AssertEqualJSON(t, []byte(`{"uuid": "1ae96956-4b34-433e-8d1a-f05fe6923d6d", "name": "New", "spec_version": "13.0.0", "type": "messaging", "language": "eng", "nodes": []}`), cloned, "cloned flow mismatch")
}

func TestMigrateDefinition(t *testing.T) {
	// 13.0 > 13.1
	migrated, err := goflow.MigrateDefinition([]byte(`{"uuid": "502c3ee4-3249-4dee-8e71-c62070667d52", "name": "New", "spec_version": "13.0.0", "type": "messaging", "language": "eng", "nodes": []}`), semver.MustParse("13.1.0"))
	assert.NoError(t, err)
	test.AssertEqualJSON(t, []byte(`{"uuid": "502c3ee4-3249-4dee-8e71-c62070667d52", "name": "New", "spec_version": "13.1.0", "type": "messaging", "language": "eng", "nodes": []}`), migrated, "migrated flow mismatch")
}
