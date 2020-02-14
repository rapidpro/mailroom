package contact

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/apex/log"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/actions"
	"github.com/nyaruka/goflow/flows/definition"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/goflow/utils/uuids"
	"github.com/nyaruka/mailroom/goflow"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/search"
	"github.com/nyaruka/mailroom/web"
	"github.com/pkg/errors"
)

func init() {
	web.RegisterJSONRoute(http.MethodPost, "/mr/contact/search", web.RequireAuthToken(handleSearch))
	web.RegisterJSONRoute(http.MethodPost, "/mr/contact/parse_query", web.RequireAuthToken(handleParseQuery))
	web.RegisterJSONRoute(http.MethodPost, "/mr/contact/apply_actions", web.RequireAuthToken(handleApplyActions))
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
		Sort:     "-id",
	}
	if err := utils.UnmarshalAndValidateWithLimit(r.Body, request, web.MaxRequestBytes); err != nil {
		return errors.Wrapf(err, "request failed validation"), http.StatusBadRequest, nil
	}

	// grab our org
	org, err := models.GetOrgAssetsWithRefresh(s.CTX, s.DB, request.OrgID, models.RefreshFields)
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
//     "group_uuid": "123123-123-123-"
//   }
//
type parseRequest struct {
	OrgID     models.OrgID     `json:"org_id"     validate:"required"`
	Query     string           `json:"query"      validate:"required"`
	GroupUUID assets.GroupUUID `json:"group_uuid"`
}

// Response for a parse query request
//
// {
//   "query": "age > 10",
//   "fields": ["age"],
//   "elastic_query": { .. }
// }
type parseResponse struct {
	Query        string      `json:"query"`
	Fields       []string    `json:"fields"`
	ElasticQuery interface{} `json:"elastic_query"`
}

// handles a query parsing request
func handleParseQuery(ctx context.Context, s *web.Server, r *http.Request) (interface{}, int, error) {
	request := &parseRequest{}
	if err := utils.UnmarshalAndValidateWithLimit(r.Body, request, web.MaxRequestBytes); err != nil {
		return errors.Wrapf(err, "request failed validation"), http.StatusBadRequest, nil
	}

	// grab our org
	org, err := models.GetOrgAssetsWithRefresh(s.CTX, s.DB, request.OrgID, models.RefreshFields)
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

	eq, err := models.BuildElasticQuery(org, resolver, request.GroupUUID, parsed)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	eqj, err := eq.Source()
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	// build our response
	response := &parseResponse{
		Query:        normalized,
		Fields:       search.FieldDependencies(parsed),
		ElasticQuery: eqj,
	}

	return response, http.StatusOK, nil
}

// Request update a contact. Clients should only pass in the fields they want updated.
//
//   {
//     "org_id": 1,
//     "contact_uuid": "559d4cf7-8ed3-43db-9bbb-2be85345f87e",
//     "name": "Joe",
//     "fields": {
//        "age": "124"
//     },
//     "add_groups": [],
//     "remove_groups": []
//   }
//
type applyActionsRequest struct {
	OrgID       models.OrgID      `json:"org_id"       validate:"required"`
	ContactUUID flows.ContactUUID `json:"contact_uuid" validate:"required"`
	Actions     []json.RawMessage `json:"actions"      validate:"required"`
}

// Response for a contact update. Will return the full contact state and any errors
//
// {
//   "contact": {
//     "id": 123,
//     "contact_uuid": "559d4cf7-8ed3-43db-9bbb-2be85345f87e",
//     "name": "Joe",
//     "language": "eng",
//     "created_on": ".."
//     "urns": [ .. ],
//     "fields": {
//     }
//     "groups": [ .. ],
//   }
// }
type applyActionsResponse struct {
	Contact json.RawMessage `json:"contact"`
}

// the types of actions our apply_actions endpoind supports
var supportedTypes map[string]bool = map[string]bool{
	actions.TypeAddContactGroups: true,
	actions.TypeAddContactURN:    true,
	// actions.TypeRemoveContactURN  <-- missing
	actions.TypeRemoveContactGroups: true,
	actions.TypeSetContactChannel:   true,
	actions.TypeSetContactLanguage:  true,
	actions.TypeSetContactName:      true,
	actions.TypeSetContactTimezone:  true,
	actions.TypeSetContactField:     true,
}

// handles a request to apply the passed in actions
func handleApplyActions(ctx context.Context, s *web.Server, r *http.Request) (interface{}, int, error) {
	request := &applyActionsRequest{}
	if err := utils.UnmarshalAndValidateWithLimit(r.Body, request, web.MaxRequestBytes); err != nil {
		return errors.Wrapf(err, "request failed validation"), http.StatusBadRequest, nil
	}

	// grab our org
	org, err := models.NewOrgAssets(s.CTX, s.DB, request.OrgID, nil)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "unable to load org assets")
	}

	sa, err := models.NewSessionAssets(org)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "unable to load session assets")
	}

	// build up our actions
	as := make([]flows.Action, len(request.Actions))
	for i, a := range request.Actions {
		action, err := actions.ReadAction(a)
		if err != nil {
			return errors.Wrapf(err, "error in action: %s", string(a)), http.StatusBadRequest, nil
		}
		if !supportedTypes[action.Type()] {
			return errors.Errorf("unsupported action type: %s", action.Type), http.StatusBadRequest, nil
		}

		as[i] = action
	}

	// create a minimal flow with these actions
	entry := definition.NewNode(
		flows.NodeUUID(uuids.New()),
		as,
		nil,
		nil,
	)

	// we have our nodes, lets create our flow
	flowUUID := assets.FlowUUID(uuids.New())
	flow, err := definition.NewFlow(
		flowUUID,
		"Contact Update Flow",
		envs.Language("eng"),
		flows.FlowTypeMessaging,
		1,
		300,
		definition.NewLocalization(),
		[]flows.Node{entry},
		nil,
	)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "error building flow")
	}

	session, sprint, err := goflow.Engine().NewSession(sa, trigger)
	if err != nil {
		log.WithError(err).Errorf("error starting flow")
		continue
	}

	// build our response
	response := &applyActionsResponse{}

	return response, http.StatusOK, nil
}
