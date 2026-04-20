package msg

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/nyaruka/gocommon/i18n"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/queue"
	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/nyaruka/mailroom/core/tasks/msgs"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/web"
	"github.com/pkg/errors"
)

func init() {
	web.RegisterRoute(http.MethodPost, "/mr/msg/broadcast", web.RequireAuthToken(web.JSONPayload(handleBroadcast)))
}

// Request to send a broadcast.
//
//	{
//	  "org_id": 1,
//	  "user_id": 56,
//	  "translations": {"eng": {"text": "Hello @contact"}, "spa": {"text": "Hola @contact"}},
//	  "base_language": "eng",
//	  "group_ids": [101, 102],
//	  "contact_ids": [4646],
//	  "urns": [4646],
//	  "optin_id": 456
//	}
type broadcastRequest struct {
	OrgID        models.OrgID                `json:"org_id"        validate:"required"`
	UserID       models.UserID               `json:"user_id"       validate:"required"`
	Translations flows.BroadcastTranslations `json:"translations"  validate:"required"`
	BaseLanguage i18n.Language               `json:"base_language" validate:"required"`
	ContactIDs   []models.ContactID          `json:"contact_ids"`
	GroupIDs     []models.GroupID            `json:"group_ids"`
	URNs         []urns.URN                  `json:"urns"`
	Query        string                      `json:"query"`
	OptInID      models.OptInID              `json:"optin_id"`
}

// handles a request to create the given broadcast
func handleBroadcast(ctx context.Context, rt *runtime.Runtime, r *broadcastRequest) (any, int, error) {
	bcast := models.NewBroadcast(r.OrgID, r.Translations, models.TemplateStateUnevaluated, r.BaseLanguage, r.OptInID, r.URNs, r.ContactIDs, r.GroupIDs, r.Query, r.UserID)

	tx, err := rt.DB.BeginTxx(ctx, nil)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "error beginning transaction")
	}

	if err := models.InsertBroadcast(ctx, tx, bcast); err != nil {
		return nil, 0, errors.Wrapf(err, "error inserting broadcast")
	}

	if err := tx.Commit(); err != nil {
		return nil, 0, errors.Wrapf(err, "error committing transaction")
	}

	task := &msgs.SendBroadcastTask{Broadcast: bcast}

	rc := rt.RP.Get()
	defer rc.Close()
	err = tasks.Queue(rc, queue.BatchQueue, bcast.OrgID, task, queue.HighPriority)
	if err != nil {
		slog.Error("error queueing broadcast task", "error", err)
	}

	return map[string]any{"id": bcast.ID}, http.StatusOK, nil
}
