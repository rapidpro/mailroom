package goflow

import (
	"encoding/json"
	"sync"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/definition"
	"github.com/nyaruka/mailroom/config"
)

var migConf *definition.MigrationConfig
var migConfInit sync.Once

// ReadFlow reads a flow from the given JSON definition, migrating it if necessary
func ReadFlow(data json.RawMessage) (flows.Flow, error) {
	return definition.ReadFlow(data, MigrationConfig())
}

// MigrationConfig returns the migration configuration for flows
func MigrationConfig() *definition.MigrationConfig {
	migConfInit.Do(func() {
		migConf = &definition.MigrationConfig{BaseMediaURL: "https://" + config.Mailroom.AttachmentDomain}
	})

	return migConf
}
