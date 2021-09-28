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
	web.RegisterJSONRoute(http.MethodPost, "/mr/ticket/reopen", web.RequireAuthToken(web.WithHTTPLogs(handleReopen)))
}

// Reopens any closed tickets with the given ids
//
//   {
//     "org_id": 123,
//     "user_id": 234,
//     "ticket_ids": [1234, 2345]
//   }
//
func handleReopen(ctx context.Context, rt *runtime.Runtime, r *http.Request, l *models.HTTPLogger) (interface{}, int, error) {
	request := &bulkTicketRequest{}
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

	evts, err := models.ReopenTickets(ctx, rt, oa, request.UserID, tickets, true, l)
	if err != nil {
		return nil, http.StatusBadRequest, errors.Wrapf(err, "error reopening tickets for org: %d", request.OrgID)
	}

	return newBulkResponse(evts), http.StatusOK, nil
}
