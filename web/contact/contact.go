package contact

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/contactql"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/goflow"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/web"

	"github.com/pkg/errors"
)

func init() {
	web.RegisterJSONRoute(http.MethodPost, "/mr/contact/search", web.RequireAuthToken(handleSearch))
	web.RegisterJSONRoute(http.MethodPost, "/mr/contact/parse_query", web.RequireAuthToken(handleParseQuery))
	web.RegisterJSONRoute(http.MethodPost, "/mr/contact/modify", web.RequireAuthToken(handleModify))
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

	// deprecated
	Fields       []string `json:"fields"`
	AllowAsGroup bool     `json:"allow_as_group"`
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

	// grab our org assets
	oa, err := models.GetOrgAssetsWithRefresh(s.CTX, s.DB, request.OrgID, models.RefreshFields|models.RefreshGroups)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "unable to load org assets")
	}

	// Perform our search
	parsed, hits, total, err := models.ContactIDsForQueryPage(ctx, s.ElasticClient, oa,
		request.GroupUUID, request.Query, request.Sort, request.Offset, request.PageSize)

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
	allowAsGroup := false
	fields := make([]string, 0)

	if parsed != nil {
		normalized = parsed.String()
		metadata = contactql.Inspect(parsed)
		fields = append(fields, metadata.Attributes...)
		for _, f := range metadata.Fields {
			fields = append(fields, f.Key)
		}
		allowAsGroup = metadata.AllowAsGroup
	}

	// build our response
	response := &searchResponse{
		Query:        normalized,
		ContactIDs:   hits,
		Total:        total,
		Offset:       request.Offset,
		Sort:         request.Sort,
		Metadata:     metadata,
		Fields:       fields,
		AllowAsGroup: allowAsGroup,
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

	// deprecated
	Fields       []string `json:"fields"`
	AllowAsGroup bool     `json:"allow_as_group"`
}

// handles a query parsing request
func handleParseQuery(ctx context.Context, s *web.Server, r *http.Request) (interface{}, int, error) {
	request := &parseRequest{}
	if err := utils.UnmarshalAndValidateWithLimit(r.Body, request, web.MaxRequestBytes); err != nil {
		return errors.Wrapf(err, "request failed validation"), http.StatusBadRequest, nil
	}

	// grab our org assets
	oa, err := models.GetOrgAssetsWithRefresh(s.CTX, s.DB, request.OrgID, models.RefreshFields|models.RefreshGroups)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "unable to load org assets")
	}

	env := oa.Env()
	parsed, err := contactql.ParseQuery(env, request.Query, oa.SessionAssets())

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
	allowAsGroup := false
	fields := make([]string, 0)

	if parsed != nil {
		normalized = parsed.String()
		metadata = contactql.Inspect(parsed)
		fields = append(fields, metadata.Attributes...)
		for _, f := range metadata.Fields {
			fields = append(fields, f.Key)
		}
		allowAsGroup = metadata.AllowAsGroup
	}

	eq := models.BuildElasticQuery(oa, request.GroupUUID, parsed)
	eqj, err := eq.Source()
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	// build our response
	response := &parseResponse{
		Query:        normalized,
		ElasticQuery: eqj,
		Metadata:     metadata,
		Fields:       fields,
		AllowAsGroup: allowAsGroup,
	}

	return response, http.StatusOK, nil
}

// Request that a set of contacts is modified.
//
//   {
//     "org_id": 1,
//     "user_id": 1,
//     "contact_ids": [15,235],
//     "modifiers": [{
//        "type": "groups",
//        "modification": "add",
//        "groups": [{
//            "uuid": "a8e8efdb-78ee-46e7-9eb0-6a578da3b02d",
//            "name": "Doctors"
//        }]
//     }]
//   }
//
type modifyRequest struct {
	OrgID      models.OrgID       `json:"org_id"       validate:"required"`
	UserID     models.UserID      `json:"user_id"`
	ContactIDs []models.ContactID `json:"contact_ids"  validate:"required"`
	Modifiers  []json.RawMessage  `json:"modifiers"    validate:"required"`
}

// Response for a contact update. Will return the full contact state and any errors
//
// {
//   "1000": {
//	   "contact": {
//       "id": 123,
//       "contact_uuid": "559d4cf7-8ed3-43db-9bbb-2be85345f87e",
//       "name": "Joe",
//       "language": "eng",
//       ...
//     }],
//     "events": [{
//          ....
//     }]
//   }, ...
// }
type modifyResult struct {
	Contact *flows.Contact `json:"contact"`
	Events  []flows.Event  `json:"events"`
}

// handles a request to apply the passed in actions
func handleModify(ctx context.Context, s *web.Server, r *http.Request) (interface{}, int, error) {
	request := &modifyRequest{}
	if err := utils.UnmarshalAndValidateWithLimit(r.Body, request, web.MaxRequestBytes); err != nil {
		return errors.Wrapf(err, "request failed validation"), http.StatusBadRequest, nil
	}

	// grab our org assets
	oa, err := models.GetOrgAssets(s.CTX, s.DB, request.OrgID)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "unable to load org assets")
	}

	// clone it as we will modify flows
	oa, err = oa.Clone(s.CTX, s.DB)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "unable to clone orgs")
	}

	// read the modifiers from the request
	mods, err := goflow.ReadModifiers(oa.SessionAssets(), request.Modifiers, goflow.ErrorOnMissing)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	// load our contacts
	contacts, err := models.LoadContacts(ctx, s.DB, oa, request.ContactIDs)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "unable to load contact")
	}

	results := make(map[models.ContactID]modifyResult)

	// create an environment instance with location support
	env := flows.NewEnvironment(oa.Env(), oa.SessionAssets().Locations())

	// gather up events for our contacts
	contactEvents := make(map[*flows.Contact][]flows.Event, len(contacts))

	for _, contact := range contacts {
		flowContact, err := contact.FlowContact(oa)
		if err != nil {
			return nil, http.StatusInternalServerError, errors.Wrapf(err, "error creating flow contact for contact: %d", contact.ID())
		}

		result := modifyResult{
			Contact: flowContact,
			Events:  make([]flows.Event, 0, len(mods)),
		}

		// apply our modifiers
		for _, mod := range mods {
			mod.Apply(env, oa.SessionAssets(), flowContact, func(e flows.Event) { result.Events = append(result.Events, e) })
		}

		results[contact.ID()] = result
		contactEvents[flowContact] = result.Events
	}

	err = models.HandleAndCommitEvents(ctx, s.DB, s.RP, oa, contactEvents)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	return results, http.StatusOK, nil
}
