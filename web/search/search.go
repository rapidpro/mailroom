package search

import (
	"context"
	"net/http"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/web"
	"github.com/pkg/errors"
)

func init() {
	web.RegisterJSONRoute(http.MethodPost, "/mr/search/search", web.RequireAuthToken(handleSearch))
}

// Searches the contacts for an org
//
//   {
//     "org_id": 1,
//     "group_uuid": "",
//     "search": "age > 10"
//   }
//
type searchRequest struct {
	OrgID     models.OrgID     `json:"org_id"     validate:"required"`
	GroupUUID assets.GroupUUID `json:"group_uuid" validate:"required"`
	Query     string           `json:"query"      validate:"required"`
	PageSize  int              `json:"page_size"`
	Offset    int              `json:"offset"`
}

// Response for a contact search
type searchResponse struct {
	Parsed  string             `json:"parsed"`
	Error   string             `json:"error"`
	Results []models.ContactID `json:"results"`
	Total   int64              `json:"total"`
	Offset  int                `json:"offset"`
}

// handles a a contact search request
func handleSearch(ctx context.Context, s *web.Server, r *http.Request) (interface{}, int, error) {
	request := &searchRequest{}
	if err := utils.UnmarshalAndValidateWithLimit(r.Body, request, web.MaxRequestBytes); err != nil {
		return nil, http.StatusBadRequest, errors.Wrapf(err, "request failed validation")
	}

	// grab our org
	org, err := models.NewOrgAssets(s.CTX, s.DB, request.OrgID, nil)
	if err != nil {
		return nil, http.StatusBadRequest, errors.Wrapf(err, "unable to load org assets")
	}

	// Perform our search
	hits, total, err := models.ContactIDsForQueryPage(ctx, s.ElasticClient, org, request.GroupUUID, request.Query, request.Offset, request.PageSize)
	if err != nil {
		return nil, http.StatusServiceUnavailable, errors.Wrapf(err, "error performing query")
	}

	response := &searchResponse{
		Results: hits,
		Total:   total,
		Offset:  request.Offset,
	}

	return response, http.StatusOK, nil
}
