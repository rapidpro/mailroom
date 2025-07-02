package handler

import (
	"encoding/json"
	"fmt"

	"github.com/gomodule/redigo/redis"
	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/queue"
	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/pkg/errors"
)

// QueueHandleTask queues a single task for the given contact
func QueueHandleTask(rc redis.Conn, contactID models.ContactID, task *queue.Task) error {
	return queueHandleTask(rc, contactID, task, false)
}

// QueueTicketEvent queues a ticket event to be handled
func QueueTicketEvent(rc redis.Conn, contactID models.ContactID, evt *models.TicketEvent) error {
	eventJSON := jsonx.MustMarshal(evt)
	var task *queue.Task

	switch evt.EventType() {
	case models.TicketEventTypeClosed:
		task = &queue.Task{
			Type:     TicketClosedEventType,
			OrgID:    int(evt.OrgID()),
			Task:     eventJSON,
			QueuedOn: dates.Now(),
		}
	}

	return queueHandleTask(rc, contactID, task, false)
}

// queueHandleTask queues a single task for the passed in contact. `front` specifies whether the task
// should be inserted in front of all other tasks for that contact
func queueHandleTask(rc redis.Conn, contactID models.ContactID, task *queue.Task, front bool) error {
	// marshal our task
	taskJSON, err := json.Marshal(task)
	if err != nil {
		return errors.Wrapf(err, "error marshalling contact task")
	}

	// first push the event on our contact queue
	contactQ := fmt.Sprintf("c:%d:%d", task.OrgID, contactID)
	if front {
		_, err = redis.Int64(rc.Do("lpush", contactQ, string(taskJSON)))

	} else {
		_, err = redis.Int64(rc.Do("rpush", contactQ, string(taskJSON)))
	}
	if err != nil {
		return errors.Wrapf(err, "error adding contact event")
	}

	// then add a handle task for that contact on our global handler queue to
	err = tasks.Queue(rc, queue.HandlerQueue, models.OrgID(task.OrgID), &HandleContactEventTask{ContactID: contactID}, queue.DefaultPriority)
	if err != nil {
		return errors.Wrapf(err, "error adding handle event task")
	}
	return nil
}
