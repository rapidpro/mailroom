package msg

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
	web.InternalRoute(http.MethodPost, "/msg/search", web.JSONPayload(handleSearch))
}

// Searches messages in the OpenSearch messages index and returns the matching events from DynamoDB.
//
//	{
//	  "org_id": 1,
//	  "text": "hello"
//	}
type searchRequest struct {
	OrgID       models.OrgID      `json:"org_id"        validate:"required"`
	Text        string            `json:"text"          validate:"required"`
	ContactUUID flows.ContactUUID `json:"contact_uuid"`
}

func handleSearch(ctx context.Context, rt *runtime.Runtime, r *searchRequest) (any, int, error) {
	if rt.OS == nil {
		return nil, 0, fmt.Errorf("OpenSearch not configured")
	}

	_, err := models.GetOrgAssets(ctx, rt, r.OrgID)
	if err != nil {
		return nil, 0, fmt.Errorf("error loading org assets: %w", err)
	}

	results, total, err := search.SearchMessages(ctx, rt, r.OrgID, r.Text, r.ContactUUID)
	if err != nil {
		return nil, 0, fmt.Errorf("error searching messages: %w", err)
	}

	wrapped := make([]any, len(results))
	for i, r := range results {
		wrapped[i] = map[string]any{
			"contact": map[string]any{"uuid": r.ContactUUID},
			"event":   r.Event,
		}
	}

	return map[string]any{"total": total, "results": wrapped}, http.StatusOK, nil
}
