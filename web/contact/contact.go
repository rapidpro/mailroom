package contact

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/search"
	"github.com/nyaruka/mailroom/web"
	"github.com/pkg/errors"
)

func init() {
	web.RegisterJSONRoute(http.MethodPost, "/mr/contact/search", web.RequireAuthToken(handleSearch))
	web.RegisterJSONRoute(http.MethodPost, "/mr/contact/parse_query", web.RequireAuthToken(handleParseQuery))
	web.RegisterJSONRoute(http.MethodPost, "/mr/contact/regroup", web.RequireAuthToken(handleRegroup))
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
	org, err := models.NewOrgAssets(s.CTX, s.DB, request.OrgID, nil)
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
	org, err := models.NewOrgAssets(s.CTX, s.DB, request.OrgID, nil)
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

// Request to reevaluate the groups for a contact
//
//   {
//     "org_id": 1,
//     "contact": ....
//   }
//
type regroupRequest struct {
	OrgID   models.OrgID    `json:"org_id"     validate:"required"`
	Contact json.RawMessage `json:"contact"    validate:"required"`
}

// Response for a regroup request
//
// {
//	 "contact_uuid": "...",
//   "groups": [
//	    ...
//   ],
//   "errors": ["no such field gender", ..]
// }
type regroupResponse struct {
	ContactUUID flows.ContactUUID        `json:"contact_uuid"`
	Groups      []*assets.GroupReference `json:"groups"`
	Errors      []error                  `json:"errors,omitempty"`
}

// handles a regroup contact request
func handleRegroup(ctx context.Context, s *web.Server, r *http.Request) (interface{}, int, error) {
	request := &regroupRequest{}
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

	// try to read our contact
	contact, err := flows.ReadContact(sa, request.Contact, nil)
	if err != nil {
		return errors.Wrapf(err, "error reading contact"), http.StatusBadRequest, nil
	}

	orgGroups, _ := org.Groups()
	orgFields, _ := org.Fields()

	// errors during reevaluation don't concern us, if dynamic groups are broken then their membership won't be included
	_, _, errs := contact.ReevaluateDynamicGroups(org.Env(), flows.NewGroupAssets(orgGroups), flows.NewFieldAssets(orgFields))

	// build our final list of group references
	groups := make([]*assets.GroupReference, 0, len(contact.Groups().All()))
	for _, group := range contact.Groups().All() {
		groups = append(groups, group.Reference())
	}

	return regroupResponse{
		ContactUUID: contact.UUID(),
		Groups:      groups,
		Errors:      errs,
	}, http.StatusOK, nil
}
