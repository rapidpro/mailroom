package contact

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/core/goflow"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/web"
	"github.com/pkg/errors"
	"golang.org/x/exp/maps"
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

// Response for contact modify. Will return the full contact state and the events generated. Contacts that we couldn't
// get a lock for are returned in skipped.
//
//	{
//	  "modified": {
//	    "1001": {
//	      "contact": {
//	        "id": 123,
//	        "contact_uuid": "559d4cf7-8ed3-43db-9bbb-2be85345f87e",
//	        "name": "Joe",
//	        "language": "eng",
//	        ...
//	      },
//	      "events": [
//	        ...
//	      ]
//	    },
//	    ...
//	  },
//	  "skipped": [1006, 1007]
//	}
type modifyResult struct {
	Contact *flows.Contact `json:"contact"`
	Events  []flows.Event  `json:"events"`
}

type modifyResponse struct {
	Modified map[flows.ContactID]modifyResult `json:"modified"`
	Skipped  []models.ContactID               `json:"skipped"`
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

	results := make(map[flows.ContactID]modifyResult, len(r.ContactIDs))
	remaining := r.ContactIDs
	start := time.Now()

	for len(remaining) > 0 && time.Since(start) < time.Second*10 {
		eventsByContact, skipped, err := tryToLockAndModify(ctx, rt, oa, remaining, mods, r.UserID)
		if err != nil {
			return nil, 0, err
		}

		for flowContact, contactEvents := range eventsByContact {
			results[flowContact.ID()] = modifyResult{Contact: flowContact, Events: contactEvents}
		}

		remaining = skipped
	}

	return &modifyResponse{Modified: results, Skipped: remaining}, http.StatusOK, nil
}

func tryToLockAndModify(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, ids []models.ContactID, mods []flows.Modifier, userID models.UserID) (map[*flows.Contact][]flows.Event, []models.ContactID, error) {
	locks, skipped, err := models.LockContacts(ctx, rt, oa.OrgID(), ids, time.Second)
	if err != nil {
		return nil, nil, err
	}

	locked := maps.Keys(locks)

	defer models.UnlockContacts(rt, oa.OrgID(), locks)

	// load our contacts
	contacts, err := models.LoadContacts(ctx, rt.DB, oa, locked)
	if err != nil {
		return nil, nil, errors.Wrap(err, "unable to load contacts")
	}

	// convert to map of flow contacts to modifiers
	modifiersByContact := make(map[*flows.Contact][]flows.Modifier, len(contacts))
	for _, contact := range contacts {
		flowContact, err := contact.FlowContact(oa)
		if err != nil {
			return nil, nil, errors.Wrap(err, "error creating flow contact")
		}

		modifiersByContact[flowContact] = mods
	}

	eventsByContact, err := models.ApplyModifiers(ctx, rt, oa, userID, modifiersByContact)
	if err != nil {
		return nil, nil, err
	}

	return eventsByContact, skipped, nil
}
