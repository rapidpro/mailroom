package handler

import (
	"encoding/json"
	"fmt"

	"github.com/gomodule/redigo/redis"
	"github.com/nyaruka/goflow/flows/triggers"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/queue"
	"github.com/pkg/errors"
)

// QueueHandleTask queues a single task for the given contact
func QueueHandleTask(rc redis.Conn, contactID models.ContactID, task *queue.Task) error {
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

	return queueContactTask(rc, models.OrgID(task.OrgID), contactID)
}

// pushes a single contact task on our queue. Note this does not push the actual content of the task
// only that a task exists for the contact, addHandleTask should be used if the task has already been pushed
// off the contact specific queue.
func queueContactTask(rc redis.Conn, orgID models.OrgID, contactID models.ContactID) error {
	// create our contact event
	contactTask := &HandleEventTask{ContactID: contactID}

	// then add a handle task for that contact on our global handler queue
	err := queue.AddTask(rc, queue.HandlerQueue, queue.HandleContactEvent, int(orgID), contactTask, queue.DefaultPriority)
	if err != nil {
		return errors.Wrapf(err, "error adding handle event task")
	}
	return nil
}

// QueueTicketEvent queues an event to be handled for the given ticket
func QueueTicketEvent(rc redis.Conn, ticket *models.Ticket, eventType triggers.TicketEventType) error {
	event := models.NewTicketEvent(ticket.OrgID(), ticket.ID(), eventType)
	eventJSON, _ := json.Marshal(event)

	task := &queue.Task{
		Type:  models.TicketEventType,
		OrgID: int(ticket.OrgID()),
		Task:  eventJSON,
	}

	return queueHandleTask(rc, ticket.ContactID(), task, false)
}
