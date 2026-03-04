package contact

import (
	"context"
	"fmt"
	"net/http"

	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/search"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/web"
)

func init() {
	web.InternalRoute(http.MethodPost, "/contact/reindex", web.JSONPayload(handleReindex))
}

// Loads the given contacts from the database and reindexes them in OpenSearch.
//
//	{
//	  "org_id": 1,
//	  "contact_ids": [10000, 10001]
//	}
type reindexRequest struct {
	OrgID      models.OrgID       `json:"org_id"      validate:"required"`
	ContactIDs []models.ContactID `json:"contact_ids" validate:"required"`
}

func handleReindex(ctx context.Context, rt *runtime.Runtime, r *reindexRequest) (any, int, error) {
	if rt.Config.OSContactsIndex == "" {
		return map[string]any{"indexed": 0}, http.StatusOK, nil
	}

	oa, err := models.GetOrgAssets(ctx, rt, r.OrgID)
	if err != nil {
		return nil, 0, fmt.Errorf("error loading org assets: %w", err)
	}

	contacts, err := models.LoadContacts(ctx, rt.DB, oa, r.ContactIDs)
	if err != nil {
		return nil, 0, fmt.Errorf("error loading contacts: %w", err)
	}

	for _, c := range contacts {
		flowContact, err := c.EngineContact(oa)
		if err != nil {
			return nil, 0, fmt.Errorf("error creating flow contact: %w", err)
		}

		if err := search.IndexContact(rt, oa, flowContact); err != nil {
			return nil, 0, fmt.Errorf("error indexing contact: %w", err)
		}
	}

	return map[string]any{"indexed": len(contacts)}, http.StatusOK, nil
}
