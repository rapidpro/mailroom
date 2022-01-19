package contact

import (
	"context"
	"net/http"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/contactql"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
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
	OrgID      models.OrgID       `json:"org_id"     validate:"required"`
	GroupUUID  assets.GroupUUID   `json:"group_uuid" validate:"required"`
	ExcludeIDs []models.ContactID `json:"exclude_ids"`
	Query      string             `json:"query"`
	PageSize   int                `json:"page_size"`
	Offset     int                `json:"offset"`
	Sort       string             `json:"sort"`
}

// Response for a contact search
//
// {
//   "query": "age > 10",
//   "contact_ids": [5,10,15],
//   "total": 3,
//   "offset": 0,
//   "metadata": {
//     "fields": [
//       {"key": "age", "name": "Age"}
//     ],
//     "allow_as_group": true
//   }
// }
type searchResponse struct {
	Query      string                `json:"query"`
	ContactIDs []models.ContactID    `json:"contact_ids"`
	Total      int64                 `json:"total"`
	Offset     int                   `json:"offset"`
	Sort       string                `json:"sort"`
	Metadata   *contactql.Inspection `json:"metadata,omitempty"`
}

// handles a contact search request
func handleSearch(ctx context.Context, rt *runtime.Runtime, r *http.Request) (interface{}, int, error) {
	request := &searchRequest{
		Offset:   0,
		PageSize: 50,
		Sort:     "-id",
	}
	if err := utils.UnmarshalAndValidateWithLimit(r.Body, request, web.MaxRequestBytes); err != nil {
		return errors.Wrapf(err, "request failed validation"), http.StatusBadRequest, nil
	}

	// grab our org assets
	oa, err := models.GetOrgAssetsWithRefresh(ctx, rt, request.OrgID, models.RefreshFields|models.RefreshGroups)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "unable to load org assets")
	}

	// perform our search
	parsed, hits, total, err := models.ContactIDsForQueryPage(ctx, rt.ES, oa,
		request.GroupUUID, request.ExcludeIDs, request.Query, request.Sort, request.Offset, request.PageSize)

	if err != nil {
		isQueryError, qerr := contactql.IsQueryError(err)
		if isQueryError {
			return qerr, http.StatusBadRequest, nil
		}
		return nil, http.StatusInternalServerError, err
	}

	// normalize and inspect the query
	normalized := ""
	var metadata *contactql.Inspection

	if parsed != nil {
		normalized = parsed.String()
		metadata = contactql.Inspect(parsed)
	}

	// build our response
	response := &searchResponse{
		Query:      normalized,
		ContactIDs: hits,
		Total:      total,
		Offset:     request.Offset,
		Sort:       request.Sort,
		Metadata:   metadata,
	}

	return response, http.StatusOK, nil
}

// Request to parse the passed in query
//
//   {
//     "org_id": 1,
//     "query": "age > 10",
//     "group_uuid": "123123-123-123-"
//   }
//
type parseRequest struct {
	OrgID     models.OrgID     `json:"org_id"     validate:"required"`
	Query     string           `json:"query"      validate:"required"`
	ParseOnly bool             `json:"parse_only"`
	GroupUUID assets.GroupUUID `json:"group_uuid"`
}

// Response for a parse query request
//
// {
//   "query": "age > 10",
//   "elastic_query": { .. },
//   "metadata": {
//     "fields": [
//       {"key": "age", "name": "Age"}
//     ],
//     "allow_as_group": true
//   }
// }
type parseResponse struct {
	Query        string                `json:"query"`
	ElasticQuery interface{}           `json:"elastic_query"`
	Metadata     *contactql.Inspection `json:"metadata,omitempty"`
}

// handles a query parsing request
func handleParseQuery(ctx context.Context, rt *runtime.Runtime, r *http.Request) (interface{}, int, error) {
	request := &parseRequest{}
	if err := utils.UnmarshalAndValidateWithLimit(r.Body, request, web.MaxRequestBytes); err != nil {
		return errors.Wrapf(err, "request failed validation"), http.StatusBadRequest, nil
	}

	// grab our org assets
	oa, err := models.GetOrgAssetsWithRefresh(ctx, rt, request.OrgID, models.RefreshFields|models.RefreshGroups)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "unable to load org assets")
	}

	env := oa.Env()
	var resolver contactql.Resolver
	if !request.ParseOnly {
		resolver = oa.SessionAssets()
	}

	parsed, err := contactql.ParseQuery(env, request.Query, resolver)
	if err != nil {
		isQueryError, qerr := contactql.IsQueryError(err)
		if isQueryError {
			return qerr, http.StatusBadRequest, nil
		}
		return nil, http.StatusInternalServerError, err
	}

	// normalize and inspect the query
	normalized := parsed.String()
	metadata := contactql.Inspect(parsed)

	var elasticSource interface{}
	if !request.ParseOnly {
		eq := models.BuildElasticQuery(oa, request.GroupUUID, models.NilContactStatus, nil, parsed)
		elasticSource, err = eq.Source()
		if err != nil {
			return nil, http.StatusInternalServerError, errors.Wrap(err, "error getting elastic source")
		}
	}

	// build our response
	response := &parseResponse{
		Query:        normalized,
		ElasticQuery: elasticSource,
		Metadata:     metadata,
	}

	return response, http.StatusOK, nil
}
