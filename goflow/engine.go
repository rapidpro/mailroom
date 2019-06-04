package goflow

import (
	"sync"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/engine"
	"github.com/nyaruka/mailroom/config"
)

// Engine returns the global engine for use in mailroom
func Engine() flows.Engine {
	engInit.Do(func() {
		eng = engine.NewBuilder().
			WithDefaultUserAgent("RapidProMailroom/" + config.Mailroom.Version).
			WithMaxStepsPerSprint(config.Mailroom.MaxStepsPerSprint).
			Build()
	})

	return eng
}

var eng flows.Engine
var engInit sync.Once
