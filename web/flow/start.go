package flow

import (
	"context"
	"net/http"

	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/contactql"
	"github.com/nyaruka/goflow/flows"
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
//     "group_uuids": ["5fa925e4-edd8-4e2a-ab24-b3dbb5932ddd", "2912b95f-5b89-4d39-a2a8-5292602f357f"],
//     "contact_uuids": ["e5bb9e6f-7703-4ba1-afba-0b12791de38b"],
//     "urns": ["tel:+1234567890"],
//     "user_query": "",
//     "exclusions": {
//       "non_active": false,
//       "in_a_flow": false,
//       "started_previously": true,
//       "not_seen_recently": false
//     },
//     "sample_size": 5
//   }
//
//   {
//     "query": "(group = "No Age" OR group = "No Name" OR uuid = "e5bb9e6f-7703-4ba1-afba-0b12791de38b" OR tel = "+1234567890") AND history != \"Registration\"",
//     "count": 567,
//     "sample": [12, 34, 56, 67, 78],
//     "metadata": {
//       "fields": [
//         {"key": "age", "name": "Age"}
//       ],
//       "allow_as_group": true
//     }
//   }
//
type previewStartRequest struct {
	OrgID        models.OrgID        `json:"org_id"       validate:"required"`
	FlowID       models.FlowID       `json:"flow_id"      validate:"required"`
	GroupUUIDs   []assets.GroupUUID  `json:"group_uuids"`
	ContactUUIDs []flows.ContactUUID `json:"contact_uuids"`
	URNs         []urns.URN          `json:"urns"`
	Query        string              `json:"query"`
	Exclusions   search.Exclusions   `json:"exclusions"`
	SampleSize   int                 `json:"sample_size"  validate:"required"`
}

type previewStartResponse struct {
	Query    string                `json:"query"`
	Count    int                   `json:"count"`
	Sample   []models.ContactID    `json:"sample"`
	Metadata *contactql.Inspection `json:"metadata,omitempty"`
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

	groups := make([]*models.Group, 0, len(request.GroupUUIDs))
	for _, groupUUID := range request.GroupUUIDs {
		g := oa.GroupByUUID(groupUUID)
		if g != nil {
			groups = append(groups, g)
		}
	}

	query := search.BuildStartQuery(oa.Env(), flow, groups, request.ContactUUIDs, request.URNs, request.Query, request.Exclusions)
	if query == "" {
		return &previewStartResponse{Sample: []models.ContactID{}}, http.StatusOK, nil
	}

	parsedQuery, sampleIDs, count, err := search.GetContactIDsForQueryPage(ctx, rt.ES, oa, nil, nil, query, "", 0, request.SampleSize)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "error querying preview")
	}

	inspection := contactql.Inspect(parsedQuery)

	return &previewStartResponse{
		Query:    parsedQuery.String(),
		Count:    int(count),
		Sample:   sampleIDs,
		Metadata: inspection,
	}, http.StatusOK, nil
}
