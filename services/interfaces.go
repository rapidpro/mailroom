package services

import (
	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/goflow/flows"
)

type TicketService interface {
	flows.TicketService

	Forward(env envs.Environment, contact *flows.Contact, ticket *flows.Ticket, text string, logHTTP flows.HTTPLogCallback) error
}
