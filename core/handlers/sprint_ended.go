package handlers

import (
	"context"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/core/hooks"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"

	"github.com/jmoiron/sqlx"
)

func init() {
	models.RegisterEventHandler(models.TypeSprintEnded, handleSprintEnded)
}

func handleSprintEnded(ctx context.Context, rt *runtime.Runtime, tx *sqlx.Tx, oa *models.OrgAssets, scene *models.Scene, e flows.Event) error {
	event := e.(*models.SprintEndedEvent)

	// if we're in a flow type that can wait then contact current flow has potentially changed
	currentFlowChanged := scene.Session().SessionType().Interrupts() && event.Contact.CurrentFlowID() != scene.Session().CurrentFlowID()

	if currentFlowChanged {
		scene.AppendToEventPreCommitHook(hooks.CommitFlowChangesHook, scene.Session().CurrentFlowID())
	}

	// if current flow has changed then we need to update modified_on, but also if this is a new session
	// then flow history may have changed too in a way that won't be captured by a flow_entered event
	if currentFlowChanged || !event.Resumed {
		scene.AppendToEventPostCommitHook(hooks.ContactModifiedHook, event)
	}

	return nil
}
