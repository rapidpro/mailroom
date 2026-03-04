package tasks

import (
	"context"
	"fmt"
	"time"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/runner"
	"github.com/nyaruka/mailroom/runtime"
)

const (
	TypeInterruptSessionBatch = "interrupt_session_batch"

	interruptSessionBatchSize = 500
)

func init() {
	RegisterType(TypeInterruptSessionBatch, func() Task { return &InterruptSessionBatch{} })
}

// InterruptSessionBatch is our task for interrupting a batch of specific sessions. The sessions will only be modified
// if they are still the contact's waiting session when the task runs.
type InterruptSessionBatch struct {
	Sessions []models.SessionRef `json:"sessions"          validate:"required"`
	Status   flows.SessionStatus `json:"status"            validate:"required"`
	FlowID   models.FlowID       `json:"flow_id,omitempty"`
}

func (t *InterruptSessionBatch) Type() string {
	return TypeInterruptSessionBatch
}

// Timeout is the maximum amount of time the task can run for
func (t *InterruptSessionBatch) Timeout() time.Duration {
	return 10 * time.Minute
}

func (t *InterruptSessionBatch) WithAssets() models.Refresh {
	return models.RefreshNone
}

func (t *InterruptSessionBatch) Perform(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets) error {
	contactIDs := make([]models.ContactID, len(t.Sessions))
	sessions := make(map[models.ContactID]flows.SessionUUID, len(t.Sessions))
	for i, s := range t.Sessions {
		contactIDs[i] = s.ContactID
		sessions[s.ContactID] = s.UUID
	}

	if _, _, err := runner.InterruptWithLock(ctx, rt, oa, contactIDs, sessions, t.Status); err != nil {
		return fmt.Errorf("error interrupting batch of sessions: %w", err)
	}

	// if this batch was created as part of a flow interruption, decrement the remaining batches counter
	if t.FlowID != 0 {
		if _, err := FlowInterruptCounter(t.FlowID).Done(ctx, rt.VK); err != nil {
			return fmt.Errorf("error decrementing flow interrupt progress key: %w", err)
		}
	}

	return nil
}
