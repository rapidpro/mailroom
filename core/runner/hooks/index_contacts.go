package hooks

import (
	"context"
	"fmt"

	"github.com/nyaruka/goflow/flows"
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

	contacts := make([]*flows.Contact, 0, len(scenes))
	for scene := range scenes {
		contacts = append(contacts, scene.Contact)
	}

	if err := search.IndexContacts(ctx, rt, oa, contacts); err != nil {
		return fmt.Errorf("error indexing contacts: %w", err)
	}

	return nil
}
