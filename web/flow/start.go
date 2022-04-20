package flow

import (
	"context"
	"net/http"

	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/search"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/web"
	"github.com/pkg/errors"
)

func init() {
	web.RegisterJSONRoute(http.MethodPost, "/mr/flow/preview_start", web.RequireAuthToken(handlePreviewStart))
}

// Generates a preview of which contacts will be started in the given flow.
//
//   {
//     "org_id": 1,
//     "flow_id": 2,
//     "group_ids": [123, 345],
//     "contact_ids": [12, 34],
//     "urns": ["tel:+1234567890"],
//     "user_query": "",
//     "exclusions": {
//       "non_active": false,
//       "in_a_flow": false,
//       "started_previously": true,
//       "not_seen_recently": false
//     }
//   }
//
//   {
//     "query": "(id = 12 OR id = 34 OR group = "No Age" OR group = "No Name" OR tel = "+1234567890") AND history != \"Registration\"",
//     "count": 567,
//     "sample": [12, 34, 56, 67, 78]
//   }
//
type previewStartRequest struct {
	OrgID      models.OrgID       `json:"org_id"       validate:"required"`
	FlowID     models.FlowID      `json:"flow_id"      validate:"required"`
	GroupIDs   []models.GroupID   `json:"group_ids"`
	ContactIDs []models.ContactID `json:"contact_ids"`
	URNs       []urns.URN         `json:"urns"`
	Query      string             `json:"query"`
	Exclusions search.Exclusions  `json:"exclusions"`
}

type previewStartResponse struct {
	Query  string             `json:"query"`
	Count  int                `json:"count"`
	Sample []models.ContactID `json:"sample"`
}

func handlePreviewStart(ctx context.Context, rt *runtime.Runtime, r *http.Request) (interface{}, int, error) {
	request := &previewStartRequest{}
	if err := utils.UnmarshalAndValidateWithLimit(r.Body, request, web.MaxRequestBytes); err != nil {
		return errors.Wrapf(err, "request failed validation"), http.StatusBadRequest, nil
	}

	oa, err := models.GetOrgAssets(ctx, rt, request.OrgID)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "unable to load org assets")
	}

	flow, err := oa.FlowByID(request.FlowID)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "unable to load flow")
	}

	query := search.BuildStartQuery(oa, flow, request.GroupIDs, request.ContactIDs, request.URNs, request.Query, request.Exclusions)
	if query == "" {
		return &previewStartResponse{Query: "", Count: 0, Sample: []models.ContactID{}}, http.StatusOK, nil
	}

	parsedQuery, sampleIDs, count, err := search.GetContactIDsForQueryPage(ctx, rt.ES, oa, nil, nil, query, "", 0, 5)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "error querying preview")
	}

	return &previewStartResponse{Query: parsedQuery.String(), Count: int(count), Sample: sampleIDs}, http.StatusOK, nil
}
