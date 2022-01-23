package interrupts

import (
	"context"
	"time"

	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/nyaruka/mailroom/runtime"

	"github.com/lib/pq"
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

const activeSessionIDsForChannelsSQL = `
SELECT 
	fs.id
FROM 
	flows_flowsession fs
	JOIN channels_channelconnection cc ON fs.connection_id = cc.id
WHERE
	fs.status = 'W' AND
	cc.channel_id = ANY($1);
`

const activeSessionIDsForFlowsSQL = `
SELECT 
	id
FROM 
	flows_flowsession fs
WHERE
	fs.status = 'W' AND
	fs.current_flow_id = ANY($1);
`

// Timeout is the maximum amount of time the task can run for
func (t *InterruptSessionsTask) Timeout() time.Duration {
	return time.Hour
}

func (t *InterruptSessionsTask) Perform(ctx context.Context, rt *runtime.Runtime, orgID models.OrgID) error {
	db := rt.DB

	if len(t.ContactIDs) > 0 {
		if err := models.InterruptContactSessions(ctx, db, t.ContactIDs); err != nil {
			return err
		}
	}

	sessionIDs := make(map[models.SessionID]bool)
	for _, sid := range t.SessionIDs {
		sessionIDs[sid] = true
	}

	// if we have ivr channel ids, explode those to session ids
	if len(t.ChannelIDs) > 0 {
		channelSessionIDs := make([]models.SessionID, 0, len(t.ChannelIDs))

		err := db.SelectContext(ctx, &channelSessionIDs, activeSessionIDsForChannelsSQL, pq.Array(t.ChannelIDs))
		if err != nil {
			return errors.Wrapf(err, "error selecting sessions for channels")
		}

		for _, sid := range channelSessionIDs {
			sessionIDs[sid] = true
		}
	}

	// if we have flow ids, explode those to session ids
	if len(t.FlowIDs) > 0 {
		flowSessionIDs := make([]models.SessionID, 0, len(t.FlowIDs))

		err := db.SelectContext(ctx, &flowSessionIDs, activeSessionIDsForFlowsSQL, pq.Array(t.FlowIDs))
		if err != nil {
			return errors.Wrapf(err, "error selecting sessions for flows")
		}

		for _, sid := range flowSessionIDs {
			sessionIDs[sid] = true
		}
	}

	uniqueSessionIDs := make([]models.SessionID, 0, len(sessionIDs))
	for id := range sessionIDs {
		uniqueSessionIDs = append(uniqueSessionIDs, id)
	}

	// interrupt all sessions and their associated runs
	err := models.ExitSessions(ctx, db, uniqueSessionIDs, models.SessionStatusInterrupted)
	if err != nil {
		return errors.Wrapf(err, "error interrupting sessions")
	}
	return nil
}
