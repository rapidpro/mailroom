package ticket

import (
	"context"
	"net/http"

	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/tasks/handler"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/web"
	"github.com/pkg/errors"
)

func init() {
	web.RegisterRoute(http.MethodPost, "/mr/ticket/close", web.RequireAuthToken(web.MarshaledResponse(handleClose)))
}

// Closes any open tickets with the given ids. If force=true then even if tickets can't be closed on external service,
// they are still closed locally. This is used in case of deleting a ticketing service which may no longer be functioning.
//
//	{
//	  "org_id": 123,
//	  "user_id": 234,
//	  "ticket_ids": [1234, 2345],
//	  "force": false
//	}
func handleClose(ctx context.Context, rt *runtime.Runtime, r *http.Request) (any, int, error) {
	request := &bulkTicketRequest{}
	if err := web.ReadAndValidateJSON(r, request); err != nil {
		return errors.Wrapf(err, "request failed validation"), http.StatusBadRequest, nil
	}

	// grab our org assets
	oa, err := models.GetOrgAssets(ctx, rt, request.OrgID)
	if err != nil {
		return nil, 0, errors.Wrap(err, "unable to load org assets")
	}

	tickets, err := models.LoadTickets(ctx, rt.DB, request.TicketIDs)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "error loading tickets for org: %d", request.OrgID)
	}

	evts, err := models.CloseTickets(ctx, rt, oa, request.UserID, tickets)
	if err != nil {
		return nil, 0, errors.Wrap(err, "error closing tickets")
	}

	rc := rt.RP.Get()
	defer rc.Close()

	for t, e := range evts {
		err = handler.QueueTicketEvent(rc, t.ContactID(), e)
		if err != nil {
			return nil, 0, errors.Wrapf(err, "error queueing ticket event for ticket %d", t.ID())
		}
	}

	return newBulkResponse(evts), http.StatusOK, nil
}
