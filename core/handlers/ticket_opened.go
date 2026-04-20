package handlers

import (
	"context"
	"log/slog"

	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/core/hooks"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/pkg/errors"
)

func init() {
	models.RegisterEventHandler(events.TypeTicketOpened, handleTicketOpened)
}

// handleTicketOpened is called for each ticket opened event
func handleTicketOpened(ctx context.Context, rt *runtime.Runtime, tx *sqlx.Tx, oa *models.OrgAssets, scene *models.Scene, e flows.Event) error {
	event := e.(*events.TicketOpenedEvent)

	slog.Debug("ticket opened", "contact", scene.ContactUUID(), "session", scene.SessionID(), "ticket", event.Ticket.UUID)

	var topicID models.TopicID
	if event.Ticket.Topic != nil {
		topic := oa.TopicByUUID(event.Ticket.Topic.UUID)
		if topic == nil {
			return errors.Errorf("unable to find topic with UUID: %s", event.Ticket.Topic.UUID)
		}
		topicID = topic.ID()
	}

	var assigneeID models.UserID
	if event.Ticket.Assignee != nil {
		assignee := oa.UserByEmail(event.Ticket.Assignee.Email)
		if assignee == nil {
			return errors.Errorf("unable to find user with email: %s", event.Ticket.Assignee.Email)
		}
		assigneeID = assignee.ID()
	}

	var openedInID models.FlowID
	if scene.Session() != nil {
		run, _ := scene.Session().FindStep(e.StepUUID())
		flowAsset, _ := oa.FlowByUUID(run.FlowReference().UUID)
		if flowAsset != nil {
			openedInID = flowAsset.(*models.Flow).ID()
		}
	}

	ticket := models.NewTicket(
		event.Ticket.UUID,
		oa.OrgID(),
		scene.UserID(),
		openedInID,
		scene.ContactID(),
		topicID,
		event.Ticket.Body,
		assigneeID,
	)

	scene.AppendToEventPreCommitHook(hooks.InsertTicketsHook, ticket)

	return nil
}
