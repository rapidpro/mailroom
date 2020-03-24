package goflow

import (
	"encoding/json"
	"sync"

	"github.com/greatnonprofits-nfp/goflow/flows"
	"github.com/greatnonprofits-nfp/goflow/flows/definition"
	"github.com/greatnonprofits-nfp/goflow/flows/definition/migrations"
	"github.com/greatnonprofits-nfp/goflow/utils/uuids"
	"github.com/nyaruka/mailroom/config"

	"github.com/Masterminds/semver"
)

var migConf *migrations.Config
var migConfInit sync.Once

// SpecVersion returns the current flow spec version
func SpecVersion() *semver.Version {
	return definition.CurrentSpecVersion
}

// ReadFlow reads a flow from the given JSON definition, migrating it if necessary
func ReadFlow(data json.RawMessage) (flows.Flow, error) {
	return definition.ReadFlow(data, MigrationConfig())
}

// CloneDefinition clones the given flow definition
func CloneDefinition(data json.RawMessage, depMapping map[uuids.UUID]uuids.UUID) (json.RawMessage, error) {
	return migrations.Clone(data, depMapping)
}

// MigrateDefinition migrates the given flow definition to the specified version
func MigrateDefinition(data json.RawMessage, toVersion *semver.Version) (json.RawMessage, error) {
	return migrations.MigrateToVersion(data, toVersion, MigrationConfig())
}

// MigrationConfig returns the migration configuration for flows
func MigrationConfig() *migrations.Config {
	migConfInit.Do(func() {
		migConf = &migrations.Config{BaseMediaURL: "https://" + config.Mailroom.AttachmentDomain}
	})

	return migConf
}
