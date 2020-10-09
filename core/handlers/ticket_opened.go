package handlers

import (
	"context"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/core/hooks"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/services/tickets"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func init() {
	models.RegisterEventHandler(events.TypeTicketOpened, handleTicketOpened)
}

// handleTicketOpened is called for each ticket opened event
func handleTicketOpened(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, oa *models.OrgAssets, scene *models.Scene, e flows.Event) error {
	event := e.(*events.TicketOpenedEvent)

	ticketer := oa.TicketerByUUID(event.Ticket.Ticketer.UUID)
	if ticketer == nil {
		return errors.Errorf("unable to find ticketer with UUID: %s", event.Ticket.Ticketer.UUID)
	}

	ticket := models.NewTicket(
		event.Ticket.UUID,
		oa.OrgID(),
		scene.ContactID(),
		ticketer.ID(),
		event.Ticket.ExternalID,
		event.Ticket.Subject,
		event.Ticket.Body,
		map[string]interface{}{
			"contact-uuid":    scene.Contact().UUID(),
			"contact-display": tickets.GetContactDisplay(oa.Env(), scene.Contact()),
		},
	)

	scene.AppendToEventPreCommitHook(hooks.InsertTicketsHook, ticket)

	logrus.WithFields(logrus.Fields{
		"contact_uuid":  scene.ContactUUID(),
		"session_id":    scene.SessionID(),
		"ticket_uuid":   event.Ticket.UUID,
		"ticketer_name": ticketer.Name,
		"ticketer_uuid": ticketer.UUID,
	}).Debug("ticket opened")

	return nil
}
