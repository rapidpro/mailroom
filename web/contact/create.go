package contact

import (
	"context"
	"net/http"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/web"
	"github.com/pkg/errors"
)

func init() {
	web.RegisterRoute(http.MethodPost, "/mr/contact/create", web.RequireAuthToken(web.JSONPayload(handleCreate)))
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
func handleCreate(ctx context.Context, rt *runtime.Runtime, r *createRequest) (any, int, error) {
	oa, err := models.GetOrgAssets(ctx, rt, r.OrgID)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "unable to load org assets")
	}

	c, err := SpecToCreation(r.Contact, oa.Env(), oa.SessionAssets())
	if err != nil {
		return err, http.StatusBadRequest, nil
	}

	_, contact, err := models.CreateContact(ctx, rt.DB, oa, r.UserID, c.Name, c.Language, c.URNs)
	if err != nil {
		return err, http.StatusBadRequest, nil
	}

	modifiersByContact := map[*flows.Contact][]flows.Modifier{contact: c.Mods}
	_, err = models.ApplyModifiers(ctx, rt, oa, r.UserID, modifiersByContact)
	if err != nil {
		return nil, 0, errors.Wrap(err, "error modifying new contact")
	}

	return map[string]any{"contact": contact}, http.StatusOK, nil
}
