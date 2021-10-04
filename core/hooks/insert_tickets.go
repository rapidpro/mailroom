package hooks

import (
	"context"

	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

// InsertTicketsHook is our hook for inserting tickets
var InsertTicketsHook models.EventCommitHook = &insertTicketsHook{}

type insertTicketsHook struct{}

// Apply inserts all the airtime transfers that were created
func (h *insertTicketsHook) Apply(ctx context.Context, rt *runtime.Runtime, tx *sqlx.Tx, oa *models.OrgAssets, scenes map[*models.Scene][]interface{}) error {
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

	// generate opened events for each ticket
	openEvents := make([]*models.TicketEvent, len(tickets))
	eventsByTicket := make(map[*models.Ticket]*models.TicketEvent, len(tickets))
	for i, ticket := range tickets {
		evt := models.NewTicketOpenedEvent(ticket, models.NilUserID, ticket.AssigneeID())
		openEvents[i] = evt
		eventsByTicket[ticket] = evt
	}

	// and insert those too
	err = models.InsertTicketEvents(ctx, tx, openEvents)
	if err != nil {
		return errors.Wrapf(err, "error inserting ticket opened events")
	}

	// and insert logs/notifications for those
	err = models.NotificationsFromTicketEvents(ctx, tx, oa, eventsByTicket)
	if err != nil {
		return errors.Wrapf(err, "error inserting notifications")
	}

	return nil
}
