package contact

import (
	"context"
	"net/http"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/contactql"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/search"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/web"
	"github.com/pkg/errors"
)

func init() {
	web.RegisterRoute(http.MethodPost, "/mr/contact/parse_query", web.RequireAuthToken(web.JSONPayload(handleParseQuery)))
}

// Request to parse the passed in query
//
//	{
//	  "org_id": 1,
//	  "query": "age > 10",
//	  "group_id": 234
//	}
type parseRequest struct {
	OrgID     models.OrgID     `json:"org_id"     validate:"required"`
	Query     string           `json:"query"      validate:"required"`
	ParseOnly bool             `json:"parse_only"`
	GroupID   models.GroupID   `json:"group_id"`
	GroupUUID assets.GroupUUID `json:"group_uuid"` // deprecated
}

// Response for a parse query request
//
//	{
//	  "query": "age > 10",
//	  "elastic_query": { .. },
//	  "metadata": {
//	    "fields": [
//	      {"key": "age", "name": "Age"}
//	    ],
//	    "allow_as_group": true
//	  }
//	}
type parseResponse struct {
	Query        string                `json:"query"`
	ElasticQuery interface{}           `json:"elastic_query"`
	Metadata     *contactql.Inspection `json:"metadata,omitempty"`
}

// handles a query parsing request
func handleParseQuery(ctx context.Context, rt *runtime.Runtime, r *parseRequest) (any, int, error) {
	oa, err := models.GetOrgAssetsWithRefresh(ctx, rt, r.OrgID, models.RefreshFields|models.RefreshGroups)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "unable to load org assets")
	}

	var group *models.Group
	if r.GroupID != 0 {
		group = oa.GroupByID(r.GroupID)
	} else if r.GroupUUID != "" {
		group = oa.GroupByUUID(r.GroupUUID)
	}

	env := oa.Env()
	var resolver contactql.Resolver
	if !r.ParseOnly {
		resolver = oa.SessionAssets()
	}

	parsed, err := contactql.ParseQuery(env, r.Query, resolver)
	if err != nil {
		isQueryError, qerr := contactql.IsQueryError(err)
		if isQueryError {
			return qerr, http.StatusBadRequest, nil
		}
		return nil, 0, err
	}

	// normalize and inspect the query
	normalized := parsed.String()
	metadata := contactql.Inspect(parsed)

	var elasticSource interface{}
	if !r.ParseOnly {
		eq := search.BuildElasticQuery(oa, group, models.NilContactStatus, nil, parsed)
		elasticSource, err = eq.Source()
		if err != nil {
			return nil, 0, errors.Wrap(err, "error getting elastic source")
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
