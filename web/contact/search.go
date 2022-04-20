package contact

import (
	"context"
	"net/http"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/contactql"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/search"
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
//     "group_id": 234,
//     "query": "age > 10",
//     "sort": "-age"
//   }
//
type searchRequest struct {
	OrgID      models.OrgID       `json:"org_id"     validate:"required"`
	GroupID    models.GroupID     `json:"group_id"`
	GroupUUID  assets.GroupUUID   `json:"group_uuid"` // deprecated
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

	var group *models.Group
	if request.GroupID != 0 {
		group = oa.GroupByID(request.GroupID)
	} else if request.GroupUUID != "" {
		group = oa.GroupByUUID(request.GroupUUID)
	}

	// perform our search
	parsed, hits, total, err := search.GetContactIDsForQueryPage(ctx, rt.ES, oa, group, request.ExcludeIDs, request.Query, request.Sort, request.Offset, request.PageSize)

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
//     "group_id": 234
//   }
//
type parseRequest struct {
	OrgID     models.OrgID     `json:"org_id"     validate:"required"`
	Query     string           `json:"query"      validate:"required"`
	ParseOnly bool             `json:"parse_only"`
	GroupID   models.GroupID   `json:"group_id"`
	GroupUUID assets.GroupUUID `json:"group_uuid"` // deprecated
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

	var group *models.Group
	if request.GroupID != 0 {
		group = oa.GroupByID(request.GroupID)
	} else if request.GroupUUID != "" {
		group = oa.GroupByUUID(request.GroupUUID)
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
		eq := search.BuildElasticQuery(oa, group, models.NilContactStatus, nil, parsed)
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
