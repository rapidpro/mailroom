package handlers

import (
	"context"

	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/core/hooks"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/sirupsen/logrus"
)

func init() {
	models.RegisterEventHandler(models.TypeContactFlowChanged, handleContactFlowChanged)
}

// handleContactFlowChanged handles contact_flow_changed events which the engine doesn't produce but we append to update a contact's current flow
func handleContactFlowChanged(ctx context.Context, rt *runtime.Runtime, tx *sqlx.Tx, oa *models.OrgAssets, scene *models.Scene, e flows.Event) error {
	event := e.(*models.ContactFlowChangedEvent)

	logrus.WithFields(logrus.Fields{"contact_uuid": scene.ContactUUID(), "session_id": scene.SessionID(), "flow_id": event.FlowID}).Debug("contact flow changed")

	scene.AppendToEventPreCommitHook(hooks.CommitFlowChangesHook, event)
	scene.AppendToEventPostCommitHook(hooks.ContactModifiedHook, event)

	return nil
}
