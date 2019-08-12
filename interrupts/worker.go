package starts

import (
	"context"
	"encoding/json"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/mailroom"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/queue"
	"github.com/olivere/elastic"
	"github.com/pkg/errors"
)

func init() {
	mailroom.AddTaskFunction(queue.InterruptSessions, handleInterruptSessions)
}

// InterruptSessionsTask is our task for interrupting sessions
type InterruptSessionsTask struct {
	SessionIDs    []models.SessionID `json:"session_ids,omitempty"`
	ContactIDs    []models.ContactID `json:"contact_ids,omitempty"`
	IVRChannelIDs []models.ChannelID `json:"ivr_channel_ids,omitempty"`
}

const activeSessionIDsForChannelsSQL = `
SELECT 
	id
FROM 
	flows_flowsession fs,
	channels_channelconnetion cc JOIN ON fs.connection_id = cc.id
WHERE
	fs.ended_on IS NULL AND
	cc.channel_id = ANY($1)
`

const activeSessionIDsForContactsSQL = `
SELECT 
	id
FROM 
	flows_flowsession fs
WHERE
	fs.ended_on IS NULL AND
	fs.contact_id = ANY($1)
`

// handleInterruptSessions interrupts all the passed in sessions
func handleInterruptSessions(ctx context.Context, mr *mailroom.Mailroom, task *queue.Task) error {
	ctx, cancel := context.WithTimeout(ctx, time.Minute*60)
	defer cancel()

	// decode our task body
	if task.Type != queue.InterruptSessions {
		return errors.Errorf("unknown event type passed to interrupt worker: %s", task.Type)
	}
	intTask := &InterruptSessionsTask{}
	err := json.Unmarshal(task.Task, intTask)
	if err != nil {
		return errors.Wrapf(err, "error unmarshalling interrupt task: %s", string(task.Task))
	}

	return InterruptSessions(ctx, mr.DB, mr.RP, mr.ElasticClient, intTask)
}

// InterruptSessions interrupts all the passed in sessions
func InterruptSessions(ctx context.Context, db *sqlx.DB, rp *redis.Pool, ec *elastic.Client, task *InterruptSessionsTask) error {
	sessionIDs := make(map[models.SessionID]bool)
	for _, sid := range task.SessionIDs {
		sessionIDs[sid] = true
	}

	// if we have ivr channel ids, explode those to session ids
	if len(task.IVRChannelIDs) > 0 {
		channelSessionIDs := make([]models.SessionID, 0, len(task.IVRChannelIDs))
		err := db.SelectContext(ctx, &channelSessionIDs, activeSessionIDsForChannelsSQL, task.IVRChannelIDs)
		if err != nil {
			return errors.Wrapf(err, "error selecting sessions for channels")
		}

		for _, sid := range channelSessionIDs {
			sessionIDs[sid] = true
		}
	}

	// if we have contact ids, explode those to session ids
	if len(task.ContactIDs) > 0 {
		contactSessionIDs := make([]models.SessionID, 0, len(task.ContactIDs))
		err := db.SelectContext(ctx, &contactSessionIDs, activeSessionIDsForContactsSQL, task.ContactIDs)
		if err != nil {
			return errors.Wrapf(err, "error selecting sessions for contacts")
		}

		for _, sid := range contactSessionIDs {
			sessionIDs[sid] = true
		}
	}

	uniqueSessionIDs := make([]models.SessionID, 0, len(sessionIDs))
	for id := range sessionIDs {
		uniqueSessionIDs = append(uniqueSessionIDs, id)
	}

	// interrupt all sessions and their associated runs
	err := models.ExitSessions(ctx, db, uniqueSessionIDs, models.ExitInterrupted, time.Now())
	if err != nil {
		return errors.Wrapf(err, "error interrupting sessions")
	}
	return nil
}
