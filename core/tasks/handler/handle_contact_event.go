package handler

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/nyaruka/gocommon/analytics"
	"github.com/nyaruka/gocommon/dbutil"
	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/queue"
	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/pkg/errors"
)

// TypeHandleContactEvent is the task type for flagging that a contact has tasks to be handled
const TypeHandleContactEvent = "handle_contact_event"

func init() {
	tasks.RegisterType(TypeHandleContactEvent, func() tasks.Task { return &HandleContactEventTask{} })
}

// HandleContactEventTask is the task to flag that a contact has tasks
type HandleContactEventTask struct {
	ContactID models.ContactID `json:"contact_id"`
}

func (t *HandleContactEventTask) Type() string {
	return TypeHandleContactEvent
}

// Timeout is the maximum amount of time the task can run for
func (t *HandleContactEventTask) Timeout() time.Duration {
	return time.Minute * 5
}

// Perform is called when an event comes in for a contact. To make sure we don't get into a situation of being off by one,
// this task ingests and handles all the events for a contact, one by one.
func (t *HandleContactEventTask) Perform(ctx context.Context, rt *runtime.Runtime, orgID models.OrgID) error {
	// try to get the lock for this contact, waiting up to 10 seconds
	locks, _, err := models.LockContacts(ctx, rt, orgID, []models.ContactID{t.ContactID}, time.Second*10)
	if err != nil {
		return errors.Wrapf(err, "error acquiring lock for contact %d", t.ContactID)
	}

	// we didn't get the lock.. requeue for later
	if len(locks) == 0 {
		rc := rt.RP.Get()
		defer rc.Close()
		err = tasks.Queue(rc, queue.HandlerQueue, orgID, &HandleContactEventTask{ContactID: t.ContactID}, queue.DefaultPriority)
		if err != nil {
			return errors.Wrapf(err, "error re-adding contact task after failing to get lock")
		}
		slog.Info("failed to get lock for contact, requeued and skipping", "org_id", orgID, "contact_id", t.ContactID)
		return nil
	}

	defer models.UnlockContacts(rt, orgID, locks)

	// read all the events for this contact, one by one
	contactQ := fmt.Sprintf("c:%d:%d", orgID, t.ContactID)
	for {
		// pop the next event off this contacts queue
		rc := rt.RP.Get()
		event, err := redis.String(rc.Do("lpop", contactQ))
		rc.Close()

		// out of tasks? that's ok, exit
		if err == redis.ErrNil {
			return nil
		}

		// real error? report
		if err != nil {
			return errors.Wrapf(err, "error popping contact event")
		}

		start := time.Now()

		// decode our event, this is a normal task at its top level
		contactEvent := &queue.Task{}
		jsonx.MustUnmarshal([]byte(event), contactEvent)

		// hand off to the appropriate handler
		switch contactEvent.Type {

		case string(models.EventTypeStopContact), "stop_event":
			evt := &StopEvent{}
			jsonx.MustUnmarshal(contactEvent.Task, evt)
			err = handleStopEvent(ctx, rt, evt)

		case string(models.EventTypeNewConversation), string(models.EventTypeReferral), string(models.EventTypeMissedCall), string(models.EventTypeWelcomeMessage), string(models.EventTypeOptIn), string(models.EventTypeOptOut):
			evt := &models.ChannelEvent{}
			jsonx.MustUnmarshal(contactEvent.Task, evt)
			_, err = HandleChannelEvent(ctx, rt, models.ChannelEventType(contactEvent.Type), evt, nil)

		case MsgEventType:
			msg := &MsgEvent{}
			jsonx.MustUnmarshal(contactEvent.Task, msg)
			err = handleMsgEvent(ctx, rt, msg)

		case TicketClosedEventType:
			evt := &models.TicketEvent{}
			jsonx.MustUnmarshal(contactEvent.Task, evt)
			err = handleTicketEvent(ctx, rt, evt)

		case TimeoutEventType, ExpirationEventType:
			evt := &TimedEvent{}
			jsonx.MustUnmarshal(contactEvent.Task, evt)
			err = handleTimedEvent(ctx, rt, contactEvent.Type, evt)

		case MsgDeletedType:
			evt := &MsgDeletedEvent{}
			jsonx.MustUnmarshal(contactEvent.Task, evt)
			err = handleMsgDeletedEvent(ctx, rt, evt)

		default:
			return errors.Errorf("unknown contact event type: %s", contactEvent.Type)
		}

		// log our processing time to librato
		analytics.Gauge(fmt.Sprintf("mr.%s_elapsed", contactEvent.Type), float64(time.Since(start))/float64(time.Second))

		// and total latency for this task since it was queued
		analytics.Gauge(fmt.Sprintf("mr.%s_latency", contactEvent.Type), float64(time.Since(contactEvent.QueuedOn))/float64(time.Second))

		// if we get an error processing an event, requeue it for later and return our error
		if err != nil {
			log := slog.With("org_id", orgID, "contact_id", t.ContactID, "event", event)

			if qerr := dbutil.AsQueryError(err); qerr != nil {
				query, params := qerr.Query()
				log = log.With("sql", query, "sql_params", params)
			}

			contactEvent.ErrorCount++
			if contactEvent.ErrorCount < 3 {
				rc := rt.RP.Get()
				retryErr := queueHandleTask(rc, t.ContactID, contactEvent, true)
				if retryErr != nil {
					slog.Error("error requeuing errored contact event", "error", retryErr)
				}
				rc.Close()

				log.Error("error handling contact event", "error", err, "error_count", contactEvent.ErrorCount)
				return nil
			}
			log.Error("error handling contact event, permanent failure", "error", err)
			return nil
		}
	}
}
