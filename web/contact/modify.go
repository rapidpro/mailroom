package contact

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/core/goflow"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/web"
	"github.com/pkg/errors"
)

func init() {
	web.RegisterRoute(http.MethodPost, "/mr/contact/modify", web.RequireAuthToken(web.JSONPayload(handleModify)))
}

// Request that a set of contacts is modified.
//
//	{
//	  "org_id": 1,
//	  "user_id": 1,
//	  "contact_ids": [15,235],
//	  "modifiers": [{
//	     "type": "groups",
//	     "modification": "add",
//	     "groups": [{
//	         "uuid": "a8e8efdb-78ee-46e7-9eb0-6a578da3b02d",
//	         "name": "Doctors"
//	     }]
//	  }]
//	}
type modifyRequest struct {
	OrgID      models.OrgID       `json:"org_id"      validate:"required"`
	UserID     models.UserID      `json:"user_id"     validate:"required"`
	ContactIDs []models.ContactID `json:"contact_ids" validate:"required"`
	Modifiers  []json.RawMessage  `json:"modifiers"   validate:"required"`
}

// Response for a contact update. Will return the full contact state and any errors
//
//	{
//	  "1000": {
//		   "contact": {
//	      "id": 123,
//	      "contact_uuid": "559d4cf7-8ed3-43db-9bbb-2be85345f87e",
//	      "name": "Joe",
//	      "language": "eng",
//	      ...
//	    }],
//	    "events": [{
//	         ....
//	    }]
//	  }, ...
//	}
type modifyResult struct {
	Contact *flows.Contact `json:"contact"`
	Events  []flows.Event  `json:"events"`
}

// handles a request to apply the passed in actions
func handleModify(ctx context.Context, rt *runtime.Runtime, r *modifyRequest) (any, int, error) {
	oa, err := models.GetOrgAssets(ctx, rt, r.OrgID)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "unable to load org assets")
	}

	// read the modifiers from the request
	mods, err := goflow.ReadModifiers(oa.SessionAssets(), r.Modifiers, goflow.ErrorOnMissing)
	if err != nil {
		return nil, 0, err
	}

	// load our contacts
	contacts, err := models.LoadContacts(ctx, rt.DB, oa, r.ContactIDs)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "unable to load contact")
	}

	// convert to map of flow contacts to modifiers
	modifiersByContact := make(map[*flows.Contact][]flows.Modifier, len(contacts))
	for _, contact := range contacts {
		flowContact, err := contact.FlowContact(oa)
		if err != nil {
			return nil, 0, errors.Wrapf(err, "error creating flow contact for contact: %d", contact.ID())
		}

		modifiersByContact[flowContact] = mods
	}

	eventsByContact, err := models.ApplyModifiers(ctx, rt, oa, r.UserID, modifiersByContact)
	if err != nil {
		return nil, 0, err
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
