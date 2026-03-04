package contact

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/nyaruka/gocommon/aws/osearch"
	"github.com/nyaruka/gocommon/dates"
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

		doc := search.NewContactDoc(oa, flowContact)

		body, err := json.Marshal(doc)
		if err != nil {
			return nil, 0, fmt.Errorf("error marshalling contact doc: %w", err)
		}

		rt.OS.Writer.Queue(&osearch.Document{
			Index:   rt.Config.OSContactsIndex,
			ID:      string(doc.UUID),
			Routing: fmt.Sprintf("%d", doc.OrgID),
			Version: dates.Now().UnixNano(),
			Body:    body,
		})
	}

	return map[string]any{"indexed": len(contacts)}, http.StatusOK, nil
}
