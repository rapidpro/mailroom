package flow

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/core/models"
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
//     "contact_ids": [12, 34],
//     "group_ids": [123, 345],
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
	ContactIDs []models.ContactID `json:"contact_ids"`
	GroupIDs   []models.GroupID   `json:"group_ids"`
	URNs       []urns.URN         `json:"urns"`
	Query      string             `json:"query"`
	Exclusions Exclusions         `json:"exclusions"`
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

	query := BuildStartQuery(oa, flow, request.Query, request.GroupIDs, request.ContactIDs, request.URNs, request.Exclusions)
	if query == "" {
		return &previewStartResponse{Query: "", Count: 0, Sample: []models.ContactID{}}, http.StatusOK, nil
	}

	parsedQuery, sampleIDs, count, err := models.GetContactIDsForQueryPage(ctx, rt.ES, oa, nil, nil, query, "", 0, 5)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "error querying preview")
	}

	return &previewStartResponse{Query: parsedQuery.String(), Count: int(count), Sample: sampleIDs}, http.StatusOK, nil
}

//////////////// Query building support //////////////

// Exclusions are preset exclusion conditions
type Exclusions struct {
	NonActive         bool `json:"non_active"`         // contacts who are blocked, stopped or archived
	InAFlow           bool `json:"in_a_flow"`          // contacts who are currently in a flow (including this one)
	StartedPreviously bool `json:"started_previously"` // contacts who have been in this flow in the last 90 days
	NotSeenRecently   bool `json:"not_seen_recently"`  // contacts who have not been seen for more than 90 days
}

// BuildStartQuery builds a start query for the given flow and start options
func BuildStartQuery(oa *models.OrgAssets, flow *models.Flow, userQuery string, groupIDs []models.GroupID, contactIDs []models.ContactID, urnz []urns.URN, excs Exclusions) string {
	inclusions := make([]string, 0, 10)
	if userQuery != "" {
		inclusions = append(inclusions, fmt.Sprintf("(%s)", userQuery))
	}
	for _, groupID := range groupIDs {
		group := oa.GroupByID(groupID)
		if group != nil {
			inclusions = append(inclusions, fmt.Sprintf("group = \"%s\"", group.Name()))
		}
	}
	for _, contactID := range contactIDs {
		inclusions = append(inclusions, fmt.Sprintf("id = %d", contactID))
	}
	for _, urn := range urnz {
		scheme, path, _, _ := urn.ToParts()
		inclusions = append(inclusions, fmt.Sprintf("%s = \"%s\"", scheme, path))
	}

	exclusions := make([]string, 0, 10)
	if excs.NonActive {
		exclusions = append(exclusions, "status = \"A\"")
	}
	if excs.InAFlow {
		exclusions = append(exclusions, "flow = \"\"")
	}
	if excs.StartedPreviously {
		exclusions = append(exclusions, fmt.Sprintf("history != \"%s\"", flow.Name()))
	}
	if excs.NotSeenRecently {
		seenSince := dates.Now().Add(-time.Hour * 24 * 90)
		exclusions = append(exclusions, fmt.Sprintf("last_seen_on > %s", formatQueryDate(oa, seenSince)))
	}

	conditions := make([]string, 0, 10)
	if len(inclusions) == 1 {
		conditions = append(conditions, inclusions[0])
	} else if len(inclusions) > 1 {
		conditions = append(conditions, fmt.Sprintf("(%s)", strings.Join(inclusions, " OR ")))
	}
	conditions = append(conditions, exclusions...)

	return strings.Join(conditions, " AND ")
}

func formatQueryDate(oa *models.OrgAssets, t time.Time) string {
	d := dates.ExtractDate(t.In(oa.Env().Timezone()))
	s, _ := d.Format(string(oa.Env().DateFormat()), oa.Env().DefaultLocale().ToBCP47())
	return s
}
