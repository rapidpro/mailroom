package interrupts

import (
	"context"
	"time"

	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/nyaruka/mailroom/runtime"

	"github.com/pkg/errors"
)

// TypeInterruptSessions is the type of the interrupt session task
const TypeInterruptSessions = "interrupt_sessions"

func init() {
	tasks.RegisterType(TypeInterruptSessions, func() tasks.Task { return &InterruptSessionsTask{} })
}

// InterruptSessionsTask is our task for interrupting sessions
type InterruptSessionsTask struct {
	SessionIDs []models.SessionID `json:"session_ids,omitempty"`
	ContactIDs []models.ContactID `json:"contact_ids,omitempty"`
	ChannelIDs []models.ChannelID `json:"channel_ids,omitempty"`
	FlowIDs    []models.FlowID    `json:"flow_ids,omitempty"`
}

// Timeout is the maximum amount of time the task can run for
func (t *InterruptSessionsTask) Timeout() time.Duration {
	return time.Hour
}

func (t *InterruptSessionsTask) Perform(ctx context.Context, rt *runtime.Runtime, orgID models.OrgID) error {
	db := rt.DB

	if len(t.ContactIDs) > 0 {
		if err := models.InterruptSessionsForContacts(ctx, db, t.ContactIDs); err != nil {
			return err
		}
	}
	if len(t.ChannelIDs) > 0 {
		if err := models.InterruptSessionsForChannels(ctx, db, t.ChannelIDs); err != nil {
			return err
		}
	}
	if len(t.FlowIDs) > 0 {
		if err := models.InterruptSessionsForFlows(ctx, db, t.FlowIDs); err != nil {
			return err
		}
	}
	if len(t.SessionIDs) > 0 {
		if err := models.ExitSessions(ctx, db, t.SessionIDs, models.SessionStatusInterrupted); err != nil {
			return errors.Wrapf(err, "error interrupting sessions")
		}
	}

	return nil
}
