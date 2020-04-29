package hooks

import (
	"context"
	"time"

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
	// gather all our tickets and contact IDs
	tickets := make([]*models.Ticket, 0, len(scenes))
	contactIDs := make([]models.ContactID, 0, len(scenes))

	for _, ts := range scenes {
		for _, t := range ts {
			ticket := t.(*models.Ticket)
			tickets = append(tickets, ticket)
			contactIDs = append(contactIDs, ticket.ContactID())
		}
	}

	// close any open tickets belonging to these contacts
	err := models.CloseTicketsForContacts(ctx, tx, contactIDs)
	if err != nil {
		return errors.Wrapf(err, "error closing open tickets")
	}

	// insert the tickets
	err = models.InsertTickets(ctx, tx, tickets)
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
	)

	scene.AppendToEventPreCommitHook(insertTicketsHook, ticket)

	logrus.WithFields(logrus.Fields{
		"contact_uuid":  scene.ContactUUID(),
		"session_id":    scene.SessionID(),
		"ticket_uuid":   event.Ticket.UUID,
		"ticketer_name": ticketer.Name,
		"ticketer_uuid": ticketer.UUID,
	}).Debug("ticket opened")

	// create a log for each HTTP call
	for _, httpLog := range event.HTTPLogs {
		log := models.NewTicketerCalledLog(
			org.OrgID(),
			ticketer.ID(),
			httpLog.URL,
			httpLog.Request,
			httpLog.Response,
			httpLog.Status != flows.CallStatusSuccess,
			time.Duration(httpLog.ElapsedMS)*time.Millisecond,
			httpLog.CreatedOn,
		)

		scene.AppendToEventPreCommitHook(insertHTTPLogsHook, log)
	}

	return nil
}
