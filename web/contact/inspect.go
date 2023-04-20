package contact

import (
	"context"
	"net/http"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/web"
	"github.com/pkg/errors"
)

func init() {
	web.RegisterRoute(http.MethodPost, "/mr/contact/inspect", web.RequireAuthToken(web.JSONPayload(handleInspect)))
}

// Inspects contacts.
//
//	{
//	  "org_id": 1,
//	  "contact_ids": [10000, 10001]
//	}
type inspectRequest struct {
	OrgID      models.OrgID       `json:"org_id"      validate:"required"`
	ContactIDs []models.ContactID `json:"contact_ids" validate:"required"`
}

//	{
//	  "10000": {
//	    "urns": [
//	      {
//	        "channel": {"uuid": "5a1ae059-df67-4345-922c-2fad8a2376f2", "name": "Telegram"},
//	        "scheme": "telegram",
//	        "path": "1234567876543",
//	        "display": ""
//	      },
//	      {
//	        "channel": {"uuid": "b7aa1c23-b989-4e33-bd4c-1a8511259683", "name": "Vonage"},
//	        "scheme": "tel",
//	        "path": "+1234567890",
//	        "display": ""
//	      },
//	      {
//	        "channel": null,
//	        "scheme": "twitterid",
//	        "path": "45754875854",
//	        "display": "bobby"
//	      }
//	    ]
//	  }
//	  "10001": {
//	    "urns": []
//	  }
//	}
type urnInfo struct {
	Channel *assets.ChannelReference `json:"channel"`
	Scheme  string                   `json:"scheme"`
	Path    string                   `json:"path"`
	Display string                   `json:"display"`
}

type contactInfo struct {
	URNs []urnInfo `json:"urns"`
}

func handleInspect(ctx context.Context, rt *runtime.Runtime, r *inspectRequest) (any, int, error) {
	oa, err := models.GetOrgAssets(ctx, rt, r.OrgID)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "error loading org assets")
	}

	// load our contacts
	contacts, err := models.LoadContacts(ctx, rt.DB, oa, r.ContactIDs)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "error loading contact")
	}

	response := make(map[flows.ContactID]*contactInfo, len(contacts))

	for _, c := range contacts {
		flowContact, err := c.FlowContact(oa)
		if err != nil {
			return nil, 0, errors.Wrapf(err, "error creating flow contact")
		}

		// first add the URNs which have a corresponding channel (engine considers these destinations)
		dests := flowContact.ResolveDestinations(true)
		urnsSeen := make(map[string]bool, len(dests))
		urnInfos := make([]urnInfo, 0, len(flowContact.URNs()))

		for _, d := range dests {
			scheme, path, _, display := d.URN.URN().ToParts()
			urnInfos = append(urnInfos, urnInfo{Channel: d.Channel.Reference(), Scheme: scheme, Path: path, Display: display})
			urnsSeen[scheme+":"+path] = true
		}

		// then the rest of the unsendable URNs
		for _, u := range flowContact.URNs() {
			scheme, path, _, display := u.URN().ToParts()
			if !urnsSeen[scheme+":"+path] {
				urnInfos = append(urnInfos, urnInfo{Channel: nil, Scheme: scheme, Path: path, Display: display})
			}
		}

		response[flowContact.ID()] = &contactInfo{URNs: urnInfos}
	}

	return response, http.StatusOK, nil
}
