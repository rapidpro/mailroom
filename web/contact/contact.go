package contact

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/actions/modifiers"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/search"
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
//   "fields": ["age"],
//   "allow_as_group": true,
//   "total": 3,
//   "offset": 0
// }
type searchResponse struct {
	Query        string             `json:"query"`
	ContactIDs   []models.ContactID `json:"contact_ids"`
	Fields       []string           `json:"fields"`
	AllowAsGroup bool               `json:"allow_as_group"`
	Total        int64              `json:"total"`
	Offset       int                `json:"offset"`
	Sort         string             `json:"sort"`
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

	fields := search.FieldDependencies(parsed)

	// build our response
	response := &searchResponse{
		Query:        normalized,
		ContactIDs:   hits,
		Fields:       fields,
		AllowAsGroup: search.AllowAsGroup(fields),
		Total:        total,
		Offset:       request.Offset,
		Sort:         request.Sort,
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
//   "elastic_query": { .. },
//   "allow_as_group": true
// }
type parseResponse struct {
	Query        string      `json:"query"`
	Fields       []string    `json:"fields"`
	ElasticQuery interface{} `json:"elastic_query"`
	AllowAsGroup bool        `json:"allow_as_group"`
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

	parsed, err := search.ParseQuery(org.Env(), org.SessionAssets(), request.Query)

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

	eq, err := models.BuildElasticQuery(org, request.GroupUUID, parsed)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	eqj, err := eq.Source()
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	fields := search.FieldDependencies(parsed)

	// build our response
	response := &parseResponse{
		Query:        normalized,
		Fields:       fields,
		ElasticQuery: eqj,
		AllowAsGroup: search.AllowAsGroup(fields),
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

	// grab our org
	org, err := models.GetOrgAssets(s.CTX, s.DB, request.OrgID)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "unable to load org assets")
	}

	// clone it as we will modify flows
	org, err = org.Clone(s.CTX, s.DB)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "unable to clone orgs")
	}

	// build up our modifiers
	mods := make([]flows.Modifier, len(request.Modifiers))
	for i, m := range request.Modifiers {
		mod, err := modifiers.ReadModifier(org.SessionAssets(), m, nil)
		if err != nil {
			return errors.Wrapf(err, "error in modifier: %s", string(m)), http.StatusBadRequest, nil
		}
		mods[i] = mod
	}

	// load our contacts
	contacts, err := models.LoadContacts(ctx, s.DB, org, request.ContactIDs)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "unable to load contact")
	}

	results := make(map[models.ContactID]modifyResult)

	// create scenes for our contacts
	scenes := make([]*models.Scene, 0, len(contacts))
	for _, contact := range contacts {
		flowContact, err := contact.FlowContact(org)
		if err != nil {
			return nil, http.StatusInternalServerError, errors.Wrapf(err, "error creating flow contact for contact: %d", contact.ID())
		}

		result := modifyResult{
			Contact: flowContact,
			Events:  make([]flows.Event, 0, len(mods)),
		}

		scene := models.NewSceneForContact(flowContact)

		// apply our modifiers
		for _, mod := range mods {
			mod.Apply(org.Env(), org.SessionAssets(), flowContact, func(e flows.Event) { result.Events = append(result.Events, e) })
		}

		results[contact.ID()] = result
		scenes = append(scenes, scene)
	}

	// ok, commit all our events
	tx, err := s.DB.BeginTxx(ctx, nil)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "error starting transaction")
	}

	modifiedContactIDs := make([]models.ContactID, 0, len(contacts))

	// apply our events
	for _, scene := range scenes {
		err := models.HandleEvents(ctx, tx, s.RP, org, scene, results[scene.ContactID()].Events)
		if err != nil {
			return nil, http.StatusInternalServerError, errors.Wrapf(err, "error applying events")
		}
		if len(results[scene.ContactID()].Events) > 0 {
			modifiedContactIDs = append(modifiedContactIDs, scene.ContactID())
		}
	}

	// gather all our pre commit events, group them by hook and apply them
	err = models.ApplyEventPreCommitHooks(ctx, tx, s.RP, org, scenes)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "error applying pre commit hooks")
	}

	// apply modified_by
	err = models.UpdateContactModifiedBy(ctx, tx, modifiedContactIDs, request.UserID)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "error applying modified_by")
	}

	// commit our transaction
	err = tx.Commit()
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "error committing pre commit hooks")
	}

	tx, err = s.DB.BeginTxx(ctx, nil)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "error starting transaction for post commit")
	}

	// then apply our post commit hooks
	err = models.ApplyEventPostCommitHooks(ctx, tx, s.RP, org, scenes)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "error applying pre commit hooks")
	}

	err = tx.Commit()
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "error committing pre commit hooks")
	}

	return results, http.StatusOK, nil
}
