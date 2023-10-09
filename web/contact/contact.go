package contact

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/core/goflow"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/web"

	"github.com/pkg/errors"
)

func init() {
	web.RegisterJSONRoute(http.MethodPost, "/mr/contact/create", web.RequireAuthToken(handleCreate))
	web.RegisterJSONRoute(http.MethodPost, "/mr/contact/modify", web.RequireAuthToken(handleModify))
	web.RegisterJSONRoute(http.MethodPost, "/mr/contact/resolve", web.RequireAuthToken(handleResolve))
	web.RegisterJSONRoute(http.MethodPost, "/mr/contact/interrupt", web.RequireAuthToken(handleInterrupt))
}

// Request to create a new contact.
//
//	{
//	  "org_id": 1,
//	  "user_id": 1,
//	  "contact": {
//	    "name": "Joe Blow",
//	    "language": "eng",
//	    "urns": ["tel:+250788123123"],
//	    "fields": {"age": "39"},
//	    "groups": ["b0b778db-6657-430b-9272-989ad43a10db"]
//	  }
//	}
type createRequest struct {
	OrgID   models.OrgID        `json:"org_id"   validate:"required"`
	UserID  models.UserID       `json:"user_id"  validate:"required"`
	Contact *models.ContactSpec `json:"contact"  validate:"required"`
}

// handles a request to create the given contact
func handleCreate(ctx context.Context, rt *runtime.Runtime, r *http.Request) (interface{}, int, error) {
	request := &createRequest{}
	if err := web.ReadAndValidateJSON(r, request); err != nil {
		return errors.Wrapf(err, "request failed validation"), http.StatusBadRequest, nil
	}

	// grab our org
	oa, err := models.GetOrgAssets(ctx, rt, request.OrgID)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "unable to load org assets")
	}

	c, err := SpecToCreation(request.Contact, oa.Env(), oa.SessionAssets())
	if err != nil {
		return err, http.StatusBadRequest, nil
	}

	_, contact, err := models.CreateContact(ctx, rt.DB, oa, request.UserID, c.Name, c.Language, c.URNs)
	if err != nil {
		return err, http.StatusBadRequest, nil
	}

	modifiersByContact := map[*flows.Contact][]flows.Modifier{contact: c.Mods}
	_, err = models.ApplyModifiers(ctx, rt, oa, request.UserID, modifiersByContact)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrap(err, "error modifying new contact")
	}

	return map[string]interface{}{"contact": contact}, http.StatusOK, nil
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
func handleModify(ctx context.Context, rt *runtime.Runtime, r *http.Request) (interface{}, int, error) {
	request := &modifyRequest{}
	if err := web.ReadAndValidateJSON(r, request); err != nil {
		return errors.Wrapf(err, "request failed validation"), http.StatusBadRequest, nil
	}

	// grab our org assets
	oa, err := models.GetOrgAssets(ctx, rt, request.OrgID)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "unable to load org assets")
	}

	// read the modifiers from the request
	mods, err := goflow.ReadModifiers(oa.SessionAssets(), request.Modifiers, goflow.ErrorOnMissing)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	// load our contacts
	contacts, err := models.LoadContacts(ctx, rt.DB, oa, request.ContactIDs)
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

	eventsByContact, err := models.ApplyModifiers(ctx, rt, oa, request.UserID, modifiersByContact)
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

// Request to resolve a contact based on a channel and URN
//
//	{
//	  "org_id": 1,
//	  "channel_id": 234,
//	  "urn": "tel:+250788123123"
//	}
type resolveRequest struct {
	OrgID     models.OrgID     `json:"org_id"     validate:"required"`
	ChannelID models.ChannelID `json:"channel_id" validate:"required"`
	URN       urns.URN         `json:"urn"        validate:"required"`
}

// handles a request to resolve a contact
func handleResolve(ctx context.Context, rt *runtime.Runtime, r *http.Request) (interface{}, int, error) {
	request := &resolveRequest{}
	if err := web.ReadAndValidateJSON(r, request); err != nil {
		return errors.Wrapf(err, "request failed validation"), http.StatusBadRequest, nil
	}

	// grab our org
	oa, err := models.GetOrgAssets(ctx, rt, request.OrgID)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "unable to load org assets")
	}

	urn := request.URN.Normalize(string(oa.Env().DefaultCountry()))

	// TODO rework normalization to be idempotent because an invalid number like +2621112222 normalizes to
	// 2621112222 (invalid) and then normalizes to +12621112222 (valid)
	urn = urn.Normalize(string(oa.Env().DefaultCountry()))

	if err := urn.Validate(); err != nil {
		return errors.Wrapf(err, "URN failed validation"), http.StatusBadRequest, nil
	}

	_, contact, created, err := models.GetOrCreateContact(ctx, rt.DB, oa, []urns.URN{urn}, request.ChannelID)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "error getting or creating contact")
	}

	// find the URN on the contact
	for _, u := range contact.URNs() {
		if urn.Identity() == u.URN().Identity() {
			urn = u.URN()
			break
		}
	}

	return map[string]interface{}{
		"contact": contact,
		"urn": map[string]interface{}{
			"id":       models.GetURNInt(urn, "id"),
			"identity": urn.Identity(),
		},
		"created": created,
	}, http.StatusOK, nil
}

// Request that a single contact is interrupted. Multiple contacts should be interrupted via the task.
//
//	{
//	  "org_id": 1,
//	  "user_id": 3,
//	  "contact_id": 235
//	}
type interruptRequest struct {
	OrgID     models.OrgID     `json:"org_id"     validate:"required"`
	UserID    models.UserID    `json:"user_id"    validate:"required"`
	ContactID models.ContactID `json:"contact_id" validate:"required"`
}

// handles a request to interrupt a contact
func handleInterrupt(ctx context.Context, rt *runtime.Runtime, r *http.Request) (interface{}, int, error) {
	request := &interruptRequest{}
	if err := web.ReadAndValidateJSON(r, request); err != nil {
		return errors.Wrapf(err, "request failed validation"), http.StatusBadRequest, nil
	}

	count, err := models.InterruptSessionsForContacts(ctx, rt.DB, []models.ContactID{request.ContactID})
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "unable to interrupt contact")
	}

	return map[string]interface{}{"sessions": count}, http.StatusOK, nil
}
