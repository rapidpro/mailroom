package tasks

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	valkey "github.com/gomodule/redigo/redis"
	"github.com/nyaruka/gocommon/dbutil"
	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/runner/clocks"
	"github.com/nyaruka/mailroom/core/tasks/ctasks"
	"github.com/nyaruka/mailroom/runtime"
)

// TypePerformContactTask is the task type for flagging that a contact has queued tasks
const TypePerformContactTask = "handle_contact_event"

func init() {
	RegisterType(TypePerformContactTask, func() Task { return &ProcessContactQueue{} })
}

// ProcessContactQueue is the task to flag that a contact has tasks
type ProcessContactQueue struct {
	ContactID models.ContactID `json:"contact_id"`
}

func (t *ProcessContactQueue) Type() string {
	return TypePerformContactTask
}

// Timeout is the maximum amount of time the task can run for
func (t *ProcessContactQueue) Timeout() time.Duration {
	return time.Minute * 5
}

func (t *ProcessContactQueue) WithAssets() models.Refresh {
	return models.RefreshNone
}

// Perform is called when an event comes in for a contact. To make sure we don't get into a situation of being off by one,
// this task ingests and handles all the events for a contact, one by one.
func (t *ProcessContactQueue) Perform(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets) error {
	// try to get the lock for this contact, waiting up to 10 seconds
	locks, _, err := clocks.TryToLock(ctx, rt, oa, []models.ContactID{t.ContactID}, time.Second*10)
	if err != nil {
		return fmt.Errorf("error acquiring lock for contact %d: %w", t.ContactID, err)
	}

	// we didn't get the lock.. requeue for later
	if len(locks) == 0 {
		rt.Stats.RecordRealtimeLockFail()

		err = Queue(ctx, rt, rt.Queues.Realtime, oa.OrgID(), &ProcessContactQueue{ContactID: t.ContactID}, false)
		if err != nil {
			return fmt.Errorf("error re-adding contact task after failing to get lock: %w", err)
		}
		slog.Info("failed to get lock for contact, requeued and skipping", "org_id", oa.OrgID(), "contact_id", t.ContactID)
		return nil
	}

	defer clocks.Unlock(ctx, rt, oa, locks)

	// read all the events for this contact, one by one
	contactQ := fmt.Sprintf("c:%d:%d", oa.OrgID(), t.ContactID)
	for {
		// pop the next event off this contacts queue
		vc := rt.VK.Get()
		event, err := valkey.Bytes(vc.Do("LPOP", contactQ))
		vc.Close()

		// out of tasks? that's ok, exit
		if err == valkey.ErrNil {
			return nil
		}

		// real error? report
		if err != nil {
			return fmt.Errorf("error popping contact task: %w", err)
		}

		// decode our event, this is a normal task at its top level
		taskPayload := &ctasks.Payload{}
		jsonx.MustUnmarshal([]byte(event), taskPayload)

		ctask, err := ctasks.ReadTask(taskPayload.Type, taskPayload.Task)
		if err != nil {
			return fmt.Errorf("error reading contact task: %w", err)
		}

		start := time.Now()
		log := slog.With("contact", t.ContactID, "type", taskPayload.Type, "queued_on", taskPayload.QueuedOn, "error_count", taskPayload.ErrorCount)

		err = ctasks.Perform(ctx, rt, oa, t.ContactID, ctask)

		// record metrics
		rt.Stats.RecordContactTask(taskPayload.Type, int(oa.OrgID()), time.Since(start), time.Since(taskPayload.QueuedOn), err != nil)

		// if we get an error processing an event, requeue it for later and return our error
		if err != nil {
			if qerr := dbutil.AsQueryError(err); qerr != nil {
				query, params := qerr.Query()
				log = log.With("sql", query, "sql_params", params)
			}

			taskPayload.ErrorCount++
			if taskPayload.ErrorCount < 3 {
				retryErr := queueContact(ctx, rt, oa.OrgID(), t.ContactID, ctask, true, taskPayload.ErrorCount)
				if retryErr != nil {
					log.Error("error requeuing errored contact event", "error", retryErr)
				}

				log.Error("error handling contact event", "error", err, "error_count", taskPayload.ErrorCount)
				return nil
			}
			log.Error("error handling contact event, permanent failure", "error", err)
			return nil
		}

		log.Warn("ctask completed", "elapsed", time.Since(start), "latency", time.Since(taskPayload.QueuedOn))
	}
}

// QueueContact queues a task for the given contact
func QueueContact(ctx context.Context, rt *runtime.Runtime, orgID models.OrgID, contactID models.ContactID, task ctasks.Task) error {
	return queueContact(ctx, rt, orgID, contactID, task, false, 0)
}

func queueContact(ctx context.Context, rt *runtime.Runtime, orgID models.OrgID, contactID models.ContactID, task ctasks.Task, front bool, errorCount int) error {
	// queue this task to the contact's queue
	if err := ctasks.Queue(ctx, rt, orgID, contactID, task, front, errorCount); err != nil {
		return fmt.Errorf("error queuing contact task: %w", err)
	}

	// then add a task for that contact on our global realtime queue
	if err := Queue(ctx, rt, rt.Queues.Realtime, orgID, &ProcessContactQueue{ContactID: contactID}, false); err != nil {
		return fmt.Errorf("error queuing contact queue task: %w", err)
	}
	return nil
}
