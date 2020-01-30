package contact

import (
	"context"
	"net/http"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/search"
	"github.com/nyaruka/mailroom/web"
	"github.com/pkg/errors"
)

func init() {
	web.RegisterJSONRoute(http.MethodPost, "/mr/contact/search", web.RequireAuthToken(handleSearch))
	web.RegisterJSONRoute(http.MethodPost, "/mr/contact/parse_query", web.RequireAuthToken(handleParseQuery))
}

// Searches the contacts for an org
//
//   {
//     "org_id": 1,
//     "group_uuid": "985a83fe-2e9f-478d-a3ec-fa602d5e7ddd",
//     "query": "age > 10",
//     "sort": "-age"
//   }
//
type searchRequest struct {
	OrgID     models.OrgID     `json:"org_id"     validate:"required"`
	GroupUUID assets.GroupUUID `json:"group_uuid" validate:"required"`
	Query     string           `json:"query"`
	PageSize  int              `json:"page_size"`
	Offset    int              `json:"offset"`
	Sort      string           `json:"sort"`
}

// Response for a contact search
//
// {
//   "query": "age > 10",
//   "contact_ids": [5,10,15],
//   "fields": ["age"],
//   "total": 3,
//   "offset": 0
// }
type searchResponse struct {
	Query      string             `json:"query"`
	ContactIDs []models.ContactID `json:"contact_ids"`
	Fields     []string           `json:"fields"`
	Total      int64              `json:"total"`
	Offset     int                `json:"offset"`
	Sort       string             `json:"sort"`
}

// handles a contact search request
func handleSearch(ctx context.Context, s *web.Server, r *http.Request) (interface{}, int, error) {
	request := &searchRequest{
		Offset:   0,
		PageSize: 50,
		Sort:     "-created_on",
	}
	if err := utils.UnmarshalAndValidateWithLimit(r.Body, request, web.MaxRequestBytes); err != nil {
		return errors.Wrapf(err, "request failed validation"), http.StatusBadRequest, nil
	}

	// grab our org
	org, err := models.GetOrgAssets(s.CTX, s.DB, request.OrgID)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "unable to load org assets")
	}

	// Perform our search
	parsed, hits, total, err := models.ContactIDsForQueryPage(ctx, s.ElasticClient, org,
		request.GroupUUID, request.Query, request.Sort, request.Offset, request.PageSize)

	if err != nil {
		switch cause := errors.Cause(err).(type) {
		case *search.Error:
			return cause, http.StatusBadRequest, nil
		default:
			return nil, http.StatusInternalServerError, err
		}
	}

	// create our normalized query
	normalized := ""
	if parsed != nil {
		normalized = parsed.String()
	}

	// build our response
	response := &searchResponse{
		Query:      normalized,
		ContactIDs: hits,
		Fields:     search.FieldDependencies(parsed),
		Total:      total,
		Offset:     request.Offset,
		Sort:       request.Sort,
	}

	return response, http.StatusOK, nil
}

// Request to parse the passed in query
//
//   {
//     "org_id": 1,
//     "query": "age > 10",
//   }
//
type parseRequest struct {
	OrgID models.OrgID `json:"org_id"     validate:"required"`
	Query string       `json:"query"      validate:"required"`
}

// Response for a parse query request
//
// {
//   "query": "age > 10",
//   "fields": ["age"]
// }
type parseResponse struct {
	Query  string   `json:"query"`
	Fields []string `json:"fields"`
}

// handles a query parsing request
func handleParseQuery(ctx context.Context, s *web.Server, r *http.Request) (interface{}, int, error) {
	request := &parseRequest{}
	if err := utils.UnmarshalAndValidateWithLimit(r.Body, request, web.MaxRequestBytes); err != nil {
		return errors.Wrapf(err, "request failed validation"), http.StatusBadRequest, nil
	}

	// grab our org
	org, err := models.GetOrgAssets(s.CTX, s.DB, request.OrgID)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "unable to load org assets")
	}

	resolver := models.BuildFieldResolver(org)
	parsed, err := search.ParseQuery(org.Env(), resolver, request.Query)

	if err != nil {
		switch cause := errors.Cause(err).(type) {
		case *search.Error:
			return cause, http.StatusBadRequest, nil
		default:
			return nil, http.StatusInternalServerError, err
		}
	}

	normalized := ""
	if parsed != nil {
		normalized = parsed.String()
	}

	// build our response
	response := &parseResponse{
		Query:  normalized,
		Fields: search.FieldDependencies(parsed),
	}

	return response, http.StatusOK, nil
}
