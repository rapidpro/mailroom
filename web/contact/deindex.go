package contact

import (
	"context"
	"fmt"
	"net/http"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/search"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/web"
)

func init() {
	web.InternalRoute(http.MethodPost, "/contact/deindex", web.JSONPayload(handleDeindex))
}

// Requests de-indexing of the given contacts from Elastic and OpenSearch indexes.
//
//	{
//	  "org_id": 1,
//	  "contact_uuids": ["548f43fb-f32a-491f-abb7-0c29a453a06e", "540eb87f-57b7-4f9f-9fce-0ea6facbec08"]
//	}
type deindexRequest struct {
	OrgID        models.OrgID        `json:"org_id"        validate:"required"`
	ContactUUIDs []flows.ContactUUID `json:"contact_uuids" validate:"required"`
	ContactIDs   []models.ContactID  `json:"contact_ids"   validate:"required"` // still needed for Elastic
}

func handleDeindex(ctx context.Context, rt *runtime.Runtime, r *deindexRequest) (any, int, error) {
	deindexed, err := search.DeindexContactsByID(ctx, rt, r.OrgID, r.ContactIDs)
	if err != nil {
		return nil, 0, fmt.Errorf("error de-indexing contacts in org #%d: %w", r.OrgID, err)
	}

	if _, err := search.DeindexMessagesByContact(ctx, rt, r.OrgID, r.ContactUUIDs); err != nil {
		return nil, 0, fmt.Errorf("error de-indexing messages in org #%d: %w", r.OrgID, err)
	}

	return map[string]any{"deindexed": deindexed}, http.StatusOK, nil
}
