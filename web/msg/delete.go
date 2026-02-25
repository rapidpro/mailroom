package msg

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"net/http"
	"slices"

	"github.com/lib/pq"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/runner"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/web"
)

func init() {
	web.InternalRoute(http.MethodPost, "/msg/delete", web.JSONPayload(handleDelete))
}

// Deletes the given incoming messages.
//
//	{
//	  "org_id": 1,
//	  "msg_uuids": ["0199bada-2b39-7cac-9714-827df9ec6b91", "0199bb09-f0e9-7489-a58e-69304a7941a0"]
//	}
type deleteRequest struct {
	OrgID    models.OrgID      `json:"org_id"    validate:"required"`
	UserID   models.UserID     `json:"user_id"   validate:"required"`
	MsgUUIDs []flows.EventUUID `json:"msg_uuids" validate:"required"`
}

func handleDelete(ctx context.Context, rt *runtime.Runtime, r *deleteRequest) (any, int, error) {
	oa, err := models.GetOrgAssets(ctx, rt, r.OrgID)
	if err != nil {
		return nil, 0, fmt.Errorf("error loading org assets: %w", err)
	}

	// get the messages that will be deleted
	rows, err := rt.DB.QueryContext(ctx, `SELECT uuid, contact_id FROM msgs_msg WHERE org_id = $1 AND uuid = ANY($2) AND direction = 'I' AND visibility IN ('V', 'A') ORDER BY id`, r.OrgID, pq.Array(r.MsgUUIDs))
	if err != nil {
		return nil, 0, fmt.Errorf("error loading messages: %w", err)
	}
	defer rows.Close()

	msgsByContact := make(map[models.ContactID][]flows.EventUUID)
	for rows.Next() {
		var uuid flows.EventUUID
		var contactID models.ContactID
		if err := rows.Scan(&uuid, &contactID); err != nil {
			return nil, 0, fmt.Errorf("error scanning message row: %w", err)
		}
		msgsByContact[contactID] = append(msgsByContact[contactID], uuid)
	}

	mcs, err := models.LoadContacts(ctx, rt.DB, oa, slices.Collect(maps.Keys(msgsByContact)))
	if err != nil {
		return nil, 0, fmt.Errorf("error loading contacts: %w", err)
	}

	// for test determinism
	slices.SortFunc(mcs, func(a, b *models.Contact) int { return cmp.Compare(a.ID(), b.ID()) })

	for _, mc := range mcs {
		contact, err := mc.EngineContact(oa)
		if err != nil {
			return nil, 0, fmt.Errorf("error creating engine contact: %w", err)
		}

		scene := runner.NewScene(mc, contact)

		for _, tUUID := range msgsByContact[mc.ID()] {
			evt := events.NewMsgDeleted(tUUID, false)

			if err := scene.AddEvent(ctx, rt, oa, evt, r.UserID, ""); err != nil {
				return nil, 0, fmt.Errorf("error adding msg delete event to scene for contact %s: %w", scene.ContactUUID(), err)
			}
		}

		if err := scene.Commit(ctx, rt, oa); err != nil {
			return nil, 0, fmt.Errorf("error committing scene for contact %s: %w", scene.ContactUUID(), err)
		}
	}

	return map[string]any{}, http.StatusOK, nil
}
