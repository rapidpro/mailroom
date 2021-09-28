package goflow

import (
	"encoding/json"
	"sync"

	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/definition"
	"github.com/nyaruka/goflow/flows/definition/migrations"
	"github.com/nyaruka/mailroom/runtime"

	"github.com/Masterminds/semver"
)

var migConf *migrations.Config
var migConfInit sync.Once

// SpecVersion returns the current flow spec version
func SpecVersion() *semver.Version {
	return definition.CurrentSpecVersion
}

// ReadFlow reads a flow from the given JSON definition, migrating it if necessary
func ReadFlow(cfg *runtime.Config, data json.RawMessage) (flows.Flow, error) {
	return definition.ReadFlow(data, MigrationConfig(cfg))
}

// CloneDefinition clones the given flow definition
func CloneDefinition(data json.RawMessage, depMapping map[uuids.UUID]uuids.UUID) (json.RawMessage, error) {
	return migrations.Clone(data, depMapping)
}

// MigrateDefinition migrates the given flow definition to the specified version
func MigrateDefinition(cfg *runtime.Config, data json.RawMessage, toVersion *semver.Version) (json.RawMessage, error) {
	return migrations.MigrateToVersion(data, toVersion, MigrationConfig(cfg))
}

// MigrationConfig returns the migration configuration for flows
func MigrationConfig(cfg *runtime.Config) *migrations.Config {
	migConfInit.Do(func() {
		migConf = &migrations.Config{BaseMediaURL: "https://" + cfg.AttachmentDomain}
	})

	return migConf
}
