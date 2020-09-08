package contact

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/goflow"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/web"

	"github.com/pkg/errors"
)

func init() {
	web.RegisterJSONRoute(http.MethodPost, "/mr/contact/create", web.RequireAuthToken(handleCreate))
	web.RegisterJSONRoute(http.MethodPost, "/mr/contact/modify", web.RequireAuthToken(handleModify))
}

// Request to create a new contact.
//
//   {
//     "org_id": 1,
//     "user_id": 1,
//     "contact": {
//       "name": "Joe Blow",
//       "language": "eng",
//       "urns": ["tel:+250788123123"],
//       "fields": {"age": "39"},
//       "groups": ["b0b778db-6657-430b-9272-989ad43a10db"]
//     }
//   }
//
type createRequest struct {
	OrgID   models.OrgID  `json:"org_id"   validate:"required"`
	UserID  models.UserID `json:"user_id"`
	Contact *Spec         `json:"contact"  validate:"required"`
}

// handles a request to create the given contacts
func handleCreate(ctx context.Context, s *web.Server, r *http.Request) (interface{}, int, error) {
	request := &createRequest{}
	if err := utils.UnmarshalAndValidateWithLimit(r.Body, request, web.MaxRequestBytes); err != nil {
		return errors.Wrapf(err, "request failed validation"), http.StatusBadRequest, nil
	}

	// grab our org
	oa, err := models.GetOrgAssets(s.CTX, s.DB, request.OrgID)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "unable to load org assets")
	}

	c, err := request.Contact.Validate(oa.Env(), oa.SessionAssets())
	if err != nil {
		return err, http.StatusBadRequest, nil
	}

	_, contact, err := models.CreateContact(ctx, s.DB, oa, request.UserID, c.Name, c.Language, c.URNs)
	if err != nil {
		return err, http.StatusBadRequest, nil
	}

	modifiersByContact := map[*flows.Contact][]flows.Modifier{contact: c.Mods}
	_, err = ModifyContacts(ctx, s.DB, s.RP, oa, modifiersByContact)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrap(err, "error modifying new contact")
	}

	return map[string]interface{}{"contact": contact}, http.StatusOK, nil
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
		return nil, http.StatusBadRequest, errors.Wrapf(err, "unable to load contact")
	}

	// convert to map of flow contacts to modifiers
	modifiersByContact := make(map[*flows.Contact][]flows.Modifier, len(contacts))
	for _, contact := range contacts {
		flowContact, err := contact.FlowContact(oa)
		if err != nil {
			return nil, http.StatusBadRequest, errors.Wrapf(err, "error creating flow contact for contact: %d", contact.ID())
		}

		modifiersByContact[flowContact] = mods
	}

	eventsByContact, err := ModifyContacts(ctx, s.DB, s.RP, oa, modifiersByContact)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	// create our results
	results := make(map[flows.ContactID]modifyResult, len(contacts))
	for flowContact := range modifiersByContact {
		results[flowContact.ID()] = modifyResult{
			Contact: flowContact,
			Events:  eventsByContact[flowContact],
		}
	}

	return results, http.StatusOK, nil
}
