package ticket

import (
	"context"
	"net/http"

	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/web"
	"github.com/pkg/errors"
)

func init() {
	web.RegisterRoute(http.MethodPost, "/mr/ticket/add_note", web.RequireAuthToken(web.JSONPayload(handleAddNote)))
}

type addNoteRequest struct {
	bulkTicketRequest

	Note string `json:"note" validate:"required"`
}

// Adds the given text note to the tickets with the given ids
//
//	{
//	  "org_id": 123,
//	  "user_id": 234,
//	  "ticket_ids": [1234, 2345],
//	  "note": "spam"
//	}
func handleAddNote(ctx context.Context, rt *runtime.Runtime, r *addNoteRequest) (any, int, error) {
	oa, err := models.GetOrgAssets(ctx, rt, r.OrgID)
	if err != nil {
		return nil, 0, errors.Wrap(err, "unable to load org assets")
	}

	tickets, err := models.LoadTickets(ctx, rt.DB, r.TicketIDs)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "error loading tickets for org: %d", r.OrgID)
	}

	evts, err := models.TicketsAddNote(ctx, rt.DB, oa, r.UserID, tickets, r.Note)
	if err != nil {
		return nil, 0, errors.Wrap(err, "error adding notes to tickets")
	}

	return newBulkResponse(evts), http.StatusOK, nil
}
