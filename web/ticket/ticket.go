package ticket

import (
	"context"
	"net/http"

	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/web"

	"github.com/pkg/errors"
)

func init() {
	web.RegisterJSONRoute(http.MethodPost, "/mr/ticket/close", web.RequireAuthToken(web.WithHTTPLogs(handleClose)))
	web.RegisterJSONRoute(http.MethodPost, "/mr/ticket/reopen", web.RequireAuthToken(web.WithHTTPLogs(handleReopen)))
}

type bulkTicketRequest struct {
	OrgID     models.OrgID      `json:"org_id"      validate:"required"`
	TicketIDs []models.TicketID `json:"ticket_ids"`
}

type bulkTicketResponse struct {
	ChangedIDs []models.TicketID `json:"changed_ids"`
}

func newBulkResponse(changed []*models.Ticket) *bulkTicketResponse {
	ids := make([]models.TicketID, len(changed))
	for i := range changed {
		ids[i] = changed[i].ID()
	}
	return &bulkTicketResponse{ChangedIDs: ids}
}

// Closes any open tickets with the given ids
//
//   {
//     "org_id": 123,
//     "ticket_ids": [1234, 2345]
//   }
//
func handleClose(ctx context.Context, s *web.Server, r *http.Request, l *models.HTTPLogger) (interface{}, int, error) {
	request := &bulkTicketRequest{}
	if err := utils.UnmarshalAndValidateWithLimit(r.Body, request, web.MaxRequestBytes); err != nil {
		return errors.Wrapf(err, "request failed validation"), http.StatusBadRequest, nil
	}

	// grab our org assets
	oa, err := models.GetOrgAssets(s.CTX, s.DB, request.OrgID)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "unable to load org assets")
	}

	tickets, err := models.LoadTickets(ctx, s.DB, request.OrgID, request.TicketIDs, models.TicketStatusOpen)
	if err != nil {
		return nil, http.StatusBadRequest, errors.Wrapf(err, "error loading tickets for org: %d", request.OrgID)
	}

	err = models.CloseTickets(ctx, s.DB, oa, tickets, true, l)
	if err != nil {
		return nil, http.StatusBadRequest, errors.Wrapf(err, "error closing tickets for org: %d", request.OrgID)
	}

	return newBulkResponse(tickets), http.StatusOK, nil
}

// Reopens any closed tickets with the given ids
//
//   {
//     "org_id": 123,
//     "ticket_ids": [1234, 2345]
//   }
//
func handleReopen(ctx context.Context, s *web.Server, r *http.Request, l *models.HTTPLogger) (interface{}, int, error) {
	request := &bulkTicketRequest{}
	if err := utils.UnmarshalAndValidateWithLimit(r.Body, request, web.MaxRequestBytes); err != nil {
		return errors.Wrapf(err, "request failed validation"), http.StatusBadRequest, nil
	}

	// grab our org assets
	oa, err := models.GetOrgAssets(s.CTX, s.DB, request.OrgID)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "unable to load org assets")
	}

	tickets, err := models.LoadTickets(ctx, s.DB, request.OrgID, request.TicketIDs, models.TicketStatusClosed)
	if err != nil {
		return nil, http.StatusBadRequest, errors.Wrapf(err, "error loading tickets for org: %d", request.OrgID)
	}

	err = models.ReopenTickets(ctx, s.DB, oa, tickets, true, l)
	if err != nil {
		return nil, http.StatusBadRequest, errors.Wrapf(err, "error reopening tickets for org: %d", request.OrgID)
	}

	return newBulkResponse(tickets), http.StatusOK, nil
}
