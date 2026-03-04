package hooks

import (
	"context"
	"fmt"
	"log/slog"

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
		slog.Debug("indexing contact to opensearch", "uuid", scene.Contact.UUID(), "contact_id", scene.Contact.ID())

		if err := search.IndexContact(rt, oa, scene.Contact); err != nil {
			return fmt.Errorf("error indexing contact: %w", err)
		}
	}

	return nil
}
