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
	web.RegisterRoute(http.MethodPost, "/mr/ticket/reopen", web.RequireAuthToken(web.MarshaledResponse(web.WithHTTPLogs(handleReopen))))
}

// Reopens any closed tickets with the given ids
//
//	{
//	  "org_id": 123,
//	  "user_id": 234,
//	  "ticket_ids": [1234, 2345]
//	}
func handleReopen(ctx context.Context, rt *runtime.Runtime, r *http.Request, l *models.HTTPLogger) (any, int, error) {
	request := &bulkTicketRequest{}
	if err := web.ReadAndValidateJSON(r, request); err != nil {
		return errors.Wrap(err, "request failed validation"), http.StatusBadRequest, nil
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

	evts, err := models.ReopenTickets(ctx, rt, oa, request.UserID, tickets, true, l)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "error reopening tickets for org: %d", request.OrgID)
	}

	return newBulkResponse(evts), http.StatusOK, nil
}
