package hooks

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/nyaruka/gocommon/aws/osearch"
	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/runner"
	"github.com/nyaruka/mailroom/core/search"
	"github.com/nyaruka/mailroom/runtime"
)

// IndexContacts is our hook for indexing contacts to OpenSearch after the database transaction has committed
var IndexContacts runner.PostCommitHook = &indexContacts{}

type indexContacts struct{}

func (h *indexContacts) Order() int { return 10 }

func (h *indexContacts) Execute(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, scenes map[*runner.Scene][]any) error {
	if rt.Config.OSContactsIndex == "" {
		return nil
	}

	for scene := range scenes {
		doc := search.NewContactDoc(oa, scene.Contact)

		body, err := json.Marshal(doc)
		if err != nil {
			return fmt.Errorf("error marshalling contact doc: %w", err)
		}

		slog.Debug("indexing contact to opensearch", "uuid", doc.UUID, "contact_id", doc.LegacyID)

		rt.OS.Writer.Queue(&osearch.Document{
			Index:   rt.Config.OSContactsIndex,
			ID:      string(doc.UUID),
			Routing: fmt.Sprintf("%d", doc.OrgID),
			Version: dates.Now().UnixNano(),
			Body:    body,
		})
	}

	return nil
}
