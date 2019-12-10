package hooks

import (
	"github.com/greatnonprofits-nfp/goflow/flows/events"
	"github.com/nyaruka/mailroom/models"
)

func init() {
	models.RegisterEventHook(events.TypeLookupCalled, handleWebhookCalled)
}
