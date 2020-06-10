package hooks

import (
	"context"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/models"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func init() {
	models.RegisterEventHandler(events.TypeTicketOpened, handleTicketOpened)
}

// InsertTicketsHook is our hook for inserting tickets
type InsertTicketsHook struct{}

var insertTicketsHook = &InsertTicketsHook{}

// Apply inserts all the airtime transfers that were created
func (h *InsertTicketsHook) Apply(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, scenes map[*models.Scene][]interface{}) error {
	// gather all our tickets
	tickets := make([]*models.Ticket, 0, len(scenes))

	for _, ts := range scenes {
		for _, t := range ts {
			tickets = append(tickets, t.(*models.Ticket))
		}
	}

	// insert the tickets
	err := models.InsertTickets(ctx, tx, tickets)
	if err != nil {
		return errors.Wrapf(err, "error inserting tickets")
	}

	return nil
}

// handleTicketOpened is called for each ticket opened event
func handleTicketOpened(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, scene *models.Scene, e flows.Event) error {
	event := e.(*events.TicketOpenedEvent)

	ticketer := org.TicketerByUUID(event.Ticket.Ticketer.UUID)
	if ticketer == nil {
		return errors.Errorf("unable to find ticketer with UUID: %s", event.Ticket.Ticketer.UUID)
	}

	ticket := models.NewTicket(
		event.Ticket.UUID,
		org.OrgID(),
		scene.ContactID(),
		ticketer.ID(),
		event.Ticket.ExternalID,
		event.Ticket.Subject,
		event.Ticket.Body,
		map[string]interface{}{
			"contact-uuid":    scene.Contact().UUID(),
			"contact-display": scene.Contact().Format(org.Env()),
		},
	)

	scene.AppendToEventPreCommitHook(insertTicketsHook, ticket)

	logrus.WithFields(logrus.Fields{
		"contact_uuid":  scene.ContactUUID(),
		"session_id":    scene.SessionID(),
		"ticket_uuid":   event.Ticket.UUID,
		"ticketer_name": ticketer.Name,
		"ticketer_uuid": ticketer.UUID,
	}).Debug("ticket opened")

	return nil
}
