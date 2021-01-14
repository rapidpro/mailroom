package hooks

import (
	"context"

	"github.com/nyaruka/mailroom/core/models"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

// InsertTicketsHook is our hook for inserting tickets
var InsertTicketsHook models.EventCommitHook = &insertTicketsHook{}

type insertTicketsHook struct{}

// Apply inserts all the airtime transfers that were created
func (h *insertTicketsHook) Apply(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, oa *models.OrgAssets, scenes map[*models.Scene][]interface{}) error {
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
