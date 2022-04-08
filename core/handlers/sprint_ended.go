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

	// if contact's current flow has changed, add pseudo event to handle that
	if scene.Session().SessionType().Interrupts() && event.Contact.CurrentFlowID() != scene.Session().CurrentFlowID() {
		scene.AppendToEventPreCommitHook(hooks.CommitFlowChangesHook, scene.Session().CurrentFlowID())
	}

	// we assume that any sprint (i.e. an interaction with flows) requires an update to modified_on because
	// it will have changed current flow or flow history - this isn't strictly true but maybe not worth
	// optimizing because there the scenarios that violate this are so few
	scene.AppendToEventPostCommitHook(hooks.ContactModifiedHook, event)

	return nil
}
