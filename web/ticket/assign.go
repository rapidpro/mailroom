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
	web.RegisterRoute(http.MethodPost, "/mr/ticket/assign", web.RequireAuthToken(web.JSONPayload(handleAssign)))
}

type assignRequest struct {
	bulkTicketRequest

	AssigneeID models.UserID `json:"assignee_id"`
}

// Assigns the tickets with the given ids to the given user
//
//	{
//	  "org_id": 123,
//	  "user_id": 234,
//	  "ticket_ids": [1234, 2345],
//	  "assignee_id": 567
//	}
func handleAssign(ctx context.Context, rt *runtime.Runtime, r *assignRequest) (any, int, error) {
	oa, err := models.GetOrgAssets(ctx, rt, r.OrgID)
	if err != nil {
		return nil, 0, errors.Wrap(err, "unable to load org assets")
	}

	tickets, err := models.LoadTickets(ctx, rt.DB, r.TicketIDs)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "error loading tickets for org: %d", r.OrgID)
	}

	evts, err := models.TicketsAssign(ctx, rt.DB, oa, r.UserID, tickets, r.AssigneeID)
	if err != nil {
		return nil, 0, errors.Wrap(err, "error assigning tickets")
	}

	return newBulkResponse(evts), http.StatusOK, nil
}
