package contact

import (
	"context"
	"net/http"

	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/web"
	"github.com/pkg/errors"
)

func init() {
	web.RegisterRoute(http.MethodPost, "/mr/contact/inspect", web.RequireAuthToken(web.JSONPayload(handleInspect)))
}

// Inspects a contact.
//
//	{
//	  "org_id": 1,
//	  "contact_id": 3
//	}
type inspectRequest struct {
	OrgID     models.OrgID     `json:"org_id"     validate:"required"`
	ContactID models.ContactID `json:"contact_id" validate:"required"`
}

type destination struct {
	Channel *assets.ChannelReference `json:"channel"`
	URN     urns.URN                 `json:"urn"`
}

//	{
//	  "destinations": [
//	    {
//	      "channel": {"uuid": "5a1ae059-df67-4345-922c-2fad8a2376f2", "name": "Telegram"},
//	      "urn": "telegram:1234567876543"
//	    },
//	    {
//	      "channel": {"uuid": "b7aa1c23-b989-4e33-bd4c-1a8511259683", "name": "Vonage"},
//	      "urn": "tel:+1234567890"
//	    }
//	  ]
//	}
type inspectResponse struct {
	Destinations []destination `json:"destinations"`
}

func handleInspect(ctx context.Context, rt *runtime.Runtime, r *inspectRequest) (any, int, error) {
	oa, err := models.GetOrgAssets(ctx, rt, r.OrgID)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "error loading org assets")
	}

	// load our contact
	contact, err := models.LoadContact(ctx, rt.DB, oa, r.ContactID)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "error loading contact")
	}

	flowContact, err := contact.FlowContact(oa)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "error creating flow contact")
	}

	dests := flowContact.ResolveDestinations(true)
	destinations := make([]destination, len(dests))
	for i, d := range dests {
		destinations[i] = destination{Channel: d.Channel.Reference(), URN: d.URN.URN()}
	}

	return &inspectResponse{Destinations: destinations}, http.StatusOK, nil
}
