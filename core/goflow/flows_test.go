package goflow_test

import (
	"testing"
	"time"

	"github.com/Masterminds/semver"
	"github.com/nyaruka/gocommon/i18n"
	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/test"
	"github.com/nyaruka/mailroom/core/goflow"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/stretchr/testify/assert"
)

func TestSpecVersion(t *testing.T) {
	assert.Equal(t, semver.MustParse("14.4.0"), goflow.SpecVersion())
}

func TestReadFlow(t *testing.T) {
	_, rt := testsuite.Runtime(t)

	// try to read empty definition
	flow, err := goflow.ReadFlow(rt.Config, []byte(`{}`))
	assert.Nil(t, flow)
	assert.EqualError(t, err, "unable to read flow header: field 'uuid' is required, field 'name' is required, field 'spec_version' is required")

	// read legacy definition
	flow, err = goflow.ReadFlow(rt.Config, []byte(`{"flow_type": "M", "base_language": "eng", "action_sets": [], "metadata": {"uuid": "502c3ee4-3249-4dee-8e71-c62070667d52", "name": "Legacy"}}`))
	assert.Nil(t, err)
	assert.Equal(t, assets.FlowUUID("502c3ee4-3249-4dee-8e71-c62070667d52"), flow.UUID())
	assert.Equal(t, "Legacy", flow.Name())
	assert.Equal(t, i18n.Language("eng"), flow.Language())
	assert.Equal(t, flows.FlowTypeMessaging, flow.Type())

	// read new definition
	flow, err = goflow.ReadFlow(rt.Config, []byte(`{"uuid": "502c3ee4-3249-4dee-8e71-c62070667d52", "name": "New", "spec_version": "13.0.0", "type": "messaging", "language": "eng", "nodes": []}`))
	assert.Nil(t, err)
	assert.Equal(t, assets.FlowUUID("502c3ee4-3249-4dee-8e71-c62070667d52"), flow.UUID())
	assert.Equal(t, "New", flow.Name())
	assert.Equal(t, i18n.Language("eng"), flow.Language())
}

func TestCloneDefinition(t *testing.T) {
	uuids.SetGenerator(uuids.NewSeededGenerator(12345, time.Now))
	defer uuids.SetGenerator(uuids.DefaultGenerator)

	cloned, err := goflow.CloneDefinition([]byte(`{"uuid": "502c3ee4-3249-4dee-8e71-c62070667d52", "name": "New", "spec_version": "13.0.0", "type": "messaging", "language": "eng", "nodes": []}`), nil)
	assert.NoError(t, err)
	test.AssertEqualJSON(t, []byte(`{"uuid": "1ae96956-4b34-433e-8d1a-f05fe6923d6d", "name": "New", "spec_version": "13.0.0", "type": "messaging", "language": "eng", "nodes": []}`), cloned)
}

func TestMigrateDefinition(t *testing.T) {
	_, rt := testsuite.Runtime(t)

	uuids.SetGenerator(uuids.NewSeededGenerator(12345, time.Now))
	defer uuids.SetGenerator(uuids.DefaultGenerator)

	versions := []string{"13.0.0", "13.1.0", "13.2.0", "13.3.0", "13.4.0", "13.5.0", "13.6.0", "13.6.1", "14.0.0", "14.1.0", "14.2.0", "14.3.0", "14.4.0"}
	flowDefs := make(map[string][]byte, len(versions))
	for _, version := range versions {
		flowDefs[version] = testsuite.ReadFile(t, "testdata/migrate/"+version+".json")
	}

	// test migrating one version at a time
	for i, version := range versions[1:] {
		prevVersion := versions[i]
		migrated, err := goflow.MigrateDefinition(rt.Config, flowDefs[prevVersion], semver.MustParse(version))
		assert.NoError(t, err)
		test.AssertEqualJSON(t, flowDefs[version], migrated, "migrating from %s to %s", prevVersion, version)
	}

	// test migrating from the first version to the last
	currentVersion := versions[len(versions)-1]
	migrated, err := goflow.MigrateDefinition(rt.Config, flowDefs[versions[0]], semver.MustParse(currentVersion))
	assert.NoError(t, err)
	test.AssertEqualJSON(t, flowDefs[currentVersion], migrated)
}
