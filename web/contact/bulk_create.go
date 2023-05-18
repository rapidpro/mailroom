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
	web.RegisterRoute(http.MethodPost, "/mr/contact/bulk_create", web.RequireAuthToken(web.JSONPayload(handleBulkCreate)))
}

// Request to create new contacts.
//
//	{
//	  "org_id": 1,
//	  "user_id": 1,
//	  "specs": [
//	    {
//	      "name": "Joe Blow",
//	      "language": "eng",
//	      "urns": ["tel:+250788123123"],
//	      "fields": {"age": "39"},
//	      "groups": ["b0b778db-6657-430b-9272-989ad43a10db"]
//	    },
//	    {
//	      "name": "Frank",
//	      "language": "spa",
//	      "urns": ["tel:+250788124124"],
//	      "fields": {},
//	      "groups": []
//	    }
//	  ]
//	}
type bulkCreateRequest struct {
	OrgID  models.OrgID          `json:"org_id"  validate:"required"`
	UserID models.UserID         `json:"user_id" validate:"required"`
	Specs  []*models.ContactSpec `json:"specs"   validate:"required"`
}

// handles a request to create the given contact
func handleBulkCreate(ctx context.Context, rt *runtime.Runtime, r *bulkCreateRequest) (any, int, error) {
	oa, err := models.GetOrgAssets(ctx, rt, r.OrgID)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "unable to load org assets")
	}

	creations := make([]*Creation, len(r.Specs))
	for i, spec := range r.Specs {
		c, err := SpecToCreation(spec, oa.Env(), oa.SessionAssets())
		if err != nil {
			return err, http.StatusBadRequest, nil
		}
		creations[i] = c
	}

	modifiersByContact := make(map[*flows.Contact][]flows.Modifier, len(r.Specs))
	created := make(map[int]*flows.Contact, len(r.Specs))
	errored := make(map[int]string, len(r.Specs))
	status := http.StatusOK

	for i, c := range creations {
		_, contact, err := models.CreateContact(ctx, rt.DB, oa, r.UserID, c.Name, c.Language, c.URNs)
		if err != nil {
			errored[i] = err.Error()
			status = http.StatusMultiStatus
			continue
		}

		created[i] = contact
		modifiersByContact[contact] = c.Mods
	}

	_, err = models.ApplyModifiers(ctx, rt, oa, r.UserID, modifiersByContact)
	if err != nil {
		return nil, 0, errors.Wrap(err, "error modifying new contacts")
	}

	return map[string]any{"created": created, "errored": errored}, status, nil
}
