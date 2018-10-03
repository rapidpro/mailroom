package handler

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/juju/errors"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/queue"
)

const (
	contactEventType = "handle_event"

	channelEventType = "channel_event"
	stopEventType    = "stop_event"
	expireEventType  = "expire_event"
	timeoutEventType = "timeout_event"
	msgEventType     = "msg_event"
)

func init() {
	mailroom.AddTaskFunction(contactEventType, handleContactEvent)
}

// handleContactEvent is called when an event comes in for a contact, we pop the next event from the contact queue and handle it
func handleContactEvent(mr *mailroom.Mailroom, task *queue.Task) error {
	eventTask := &handleEventTask{}
	err := json.Unmarshal(task.Task, eventTask)
	if err != nil {
		return errors.Annotatef(err, "error decoding contact event task")
	}

	// pop the next event off this contacts queue
	rc := mr.RP.Get()
	contactQ := fmt.Sprintf("c:%d:%d", task.OrgID, eventTask.ContactID)
	event, err := redis.String(rc.Do("lpop", contactQ))
	rc.Close()
	if err != nil || event == "" {
		return errors.Annotatef(err, "error popping contact event")
	}

	// decode our event, this is a normal task at its top level
	contactEvent := &queue.Task{}
	err = json.Unmarshal([]byte(event), contactEvent)
	if err != nil {
		return errors.Annotatef(err, "error unmarshalling contact event")
	}

	// hand off to the appropriate handler
	switch contactEvent.Type {
	case channelEventType:
		return handleChannelEvent(mr.CTX, mr.DB, mr.RP, contactEvent)
	case msgEventType:
		msg := &msgEvent{}
		err := json.Unmarshal(contactEvent.Task, msg)
		if err != nil {
			return errors.Annotatef(err, "error unmarshalling msg event")
		}
		return handleMsgEvent(mr.CTX, mr.DB, mr.RP, msg)
	case stopEventType:
		stop := &stopEvent{}
		err := json.Unmarshal(contactEvent.Task, stop)
		if err != nil {
			return errors.Annotatef(err, "error unmarshalling stop event")
		}
		return handleStopEvent(mr.CTX, mr.DB, mr.RP, stop)
	default:
		return errors.Errorf("unknown contact event type: %s", contactEvent.Type)
	}
}

func handleChannelEvent(ctx context.Context, db *sqlx.DB, rp *redis.Pool, event *queue.Task) error {
	return nil
}

// handleStopEvent is called when a contact is stopped by courier
func handleStopEvent(ctx context.Context, db *sqlx.DB, rp *redis.Pool, event *stopEvent) error {
	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		return errors.Annotatef(err, "unable to start transaction for stopping contact")
	}
	err = models.StopContact(ctx, tx, event.OrgID, event.ContactID)
	if err != nil {
		tx.Rollback()
		return err
	}
	err = tx.Commit()
	if err != nil {
		return errors.Annotatef(err, "unable to commit for contact stop")
	}
	return err
}

type stopEvent struct {
	OrgID     models.OrgID    `json:"org_id"`
	ContactID flows.ContactID `json:"contact_id"`
}

// handleMsgEvent is called when a new message arrives from a contact
func handleMsgEvent(ctx context.Context, db *sqlx.DB, rp *redis.Pool, event *msgEvent) error {
	org, err := models.GetOrgAssets(ctx, db, event.OrgID)
	if err != nil {
		return errors.Annotatef(err, "error loading org")
	}

	// load our contact
	contacts, err := models.LoadContacts(ctx, db, org, []flows.ContactID{event.ContactID})
	if err != nil {
		return errors.Annotatef(err, "error loading contact")
	}

	// TODO: handle new contact
	// TODO: handle new message

	// contact has been deleted, ignore this message but mark it as handled
	if len(contacts) == 0 {
		//TODO: models.MarkMessageHandled(ctx, db, org, )
		return nil
	}

	contact := contacts[0]

	// find any matching triggers
	models.FindMatchingMsgTrigger(org, contact, event.Text)

	// get any active session for this contact
	//session := models.FindActiveSession(db, org, contact)

	// we found a trigger and their session is nil or doesn't ignore keywords

	// fire our trigger instead, starting them in the appropriate flow
	return nil
}

type msgEvent struct {
	OrgID     models.OrgID    `json:"org_id"`
	ContactID flows.ContactID `json:"contact_id"`
	MsgID     models.MsgID    `json:"msg_id"`
	Text      string          `json:"text"`
}

type handleEventTask struct {
	ContactID flows.ContactID `json:"contact_id"`
}
