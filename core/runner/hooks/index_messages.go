package hooks

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/runner"
	"github.com/nyaruka/mailroom/core/search"
	"github.com/nyaruka/mailroom/runtime"
)

// IndexMessages is our hook for indexing messages to OpenSearch
var IndexMessages runner.PostCommitHook = &indexMessages{}

type indexMessages struct{}

func (h *indexMessages) Order() int { return 10 }

func (h *indexMessages) Execute(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, scenes map[*runner.Scene][]any) error {
	for _, args := range scenes {
		for _, a := range args {
			msg := a.(*search.MessageDoc)

			doc, err := json.Marshal(msg)
			if err != nil {
				return err
			}

			slog.Debug("indexing message to opensearch", "uuid", msg.UUID, "contact", msg.ContactUUID)

			rt.OS.Messages.Queue(doc)
		}
	}

	return nil
}
