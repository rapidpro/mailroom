package hooks

import (
	"github.com/greatnonprofits-nfp/goflow/flows/events"
	"github.com/nyaruka/mailroom/models"
)

func init() {
	models.RegisterEventHandler(events.TypeLookupCalled, handleWebhookCalled)
}
