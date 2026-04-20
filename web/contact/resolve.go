package contact

import (
	"context"
	"net/http"

	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/web"
	"github.com/pkg/errors"
)

func init() {
	web.RegisterRoute(http.MethodPost, "/mr/contact/resolve", web.RequireAuthToken(web.JSONPayload(handleResolve)))
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
func handleResolve(ctx context.Context, rt *runtime.Runtime, r *resolveRequest) (any, int, error) {
	oa, err := models.GetOrgAssets(ctx, rt, r.OrgID)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "unable to load org assets")
	}

	urn := r.URN.Normalize(string(oa.Env().DefaultCountry()))

	// TODO rework normalization to be idempotent because an invalid number like +2621112222 normalizes to
	// 2621112222 (invalid) and then normalizes to +12621112222 (valid)
	urn = urn.Normalize(string(oa.Env().DefaultCountry()))

	if err := urn.Validate(); err != nil {
		return errors.Wrap(err, "URN failed validation"), http.StatusBadRequest, nil
	}

	_, contact, created, err := models.GetOrCreateContact(ctx, rt.DB, oa, []urns.URN{urn}, r.ChannelID)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "error getting or creating contact")
	}

	// find the URN on the contact
	for _, u := range contact.URNs() {
		if urn.Identity() == u.URN().Identity() {
			urn = u.URN()
			break
		}
	}

	return map[string]any{
		"contact": contact,
		"urn": map[string]any{
			"id":       models.GetURNInt(urn, "id"),
			"identity": urn.Identity(),
		},
		"created": created,
	}, http.StatusOK, nil
}
