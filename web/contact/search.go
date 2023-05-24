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
	web.RegisterRoute(http.MethodPost, "/mr/contact/search", web.RequireAuthToken(web.JSONPayload(handleSearch)))
}

// Searches the contacts for an org
//
//	{
//	  "org_id": 1,
//	  "group_id": 234,
//	  "query": "age > 10",
//	  "sort": "-age"
//	}
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
//	{
//	  "query": "age > 10",
//	  "contact_ids": [5,10,15],
//	  "total": 3,
//	  "offset": 0,
//	  "metadata": {
//	    "fields": [
//	      {"key": "age", "name": "Age"}
//	    ],
//	    "allow_as_group": true
//	  }
//	}
type SearchResponse struct {
	Query      string                `json:"query"`
	ContactIDs []models.ContactID    `json:"contact_ids"`
	Total      int64                 `json:"total"`
	Offset     int                   `json:"offset"`
	Sort       string                `json:"sort"`
	Metadata   *contactql.Inspection `json:"metadata,omitempty"`
}

// handles a contact search request
func handleSearch(ctx context.Context, rt *runtime.Runtime, r *searchRequest) (any, int, error) {
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

	// perform our search
	parsed, hits, total, err := search.GetContactIDsForQueryPage(ctx, rt, oa, group, r.ExcludeIDs, r.Query, r.Sort, r.Offset, 50)

	if err != nil {
		isQueryError, qerr := contactql.IsQueryError(err)
		if isQueryError {
			return qerr, http.StatusBadRequest, nil
		}
		return nil, 0, err
	}

	// normalize and inspect the query
	normalized := ""
	var metadata *contactql.Inspection

	if parsed != nil {
		normalized = parsed.String()
		metadata = contactql.Inspect(parsed)
	}

	// build our response
	response := &SearchResponse{
		Query:      normalized,
		ContactIDs: hits,
		Total:      total,
		Offset:     r.Offset,
		Sort:       r.Sort,
		Metadata:   metadata,
	}

	return response, http.StatusOK, nil
}
