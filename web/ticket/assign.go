package ticket

import (
	"context"
	"net/http"

	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/web"
	"github.com/pkg/errors"
)

func init() {
	web.RegisterJSONRoute(http.MethodPost, "/mr/ticket/assign", web.RequireAuthToken(handleAssign))
}

type assignRequest struct {
	bulkTicketRequest

	AssigneeID models.UserID `json:"assignee_id"`
	Note       string        `json:"note"`
}

// Assigns the tickets with the given ids to the given user
//
//   {
//     "org_id": 123,
//     "user_id": 234,
//     "ticket_ids": [1234, 2345],
//     "assignee_id": 567,
//     "note": "please look at these"
//   }
//
func handleAssign(ctx context.Context, rt *runtime.Runtime, r *http.Request) (interface{}, int, error) {
	request := &assignRequest{}
	if err := utils.UnmarshalAndValidateWithLimit(r.Body, request, web.MaxRequestBytes); err != nil {
		return errors.Wrapf(err, "request failed validation"), http.StatusBadRequest, nil
	}

	// grab our org assets
	oa, err := models.GetOrgAssets(ctx, rt, request.OrgID)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "unable to load org assets")
	}

	tickets, err := models.LoadTickets(ctx, rt.DB, request.TicketIDs)
	if err != nil {
		return nil, http.StatusBadRequest, errors.Wrapf(err, "error loading tickets for org: %d", request.OrgID)
	}

	evts, err := models.TicketsAssign(ctx, rt.DB, oa, request.UserID, tickets, request.AssigneeID, request.Note)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrap(err, "error assigning tickets")
	}

	return newBulkResponse(evts), http.StatusOK, nil
}
