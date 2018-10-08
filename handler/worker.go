package handler

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/juju/errors"
	"github.com/nyaruka/gocommon/urns"
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

	// contact has been deleted, ignore this message but mark it as handled
	if len(contacts) == 0 {
		//TODO: models.MarkMessageHandled(ctx, db, org, )
		return nil
	}

	// build session assets
	sa, err := models.GetSessionAssets(org)
	if err != nil {
		return errors.Annotatef(err, "unable to load session assets")
	}

	modelContact := contacts[0]
	contact, err := modelContact.FlowContact(org, sa)
	if err != nil {
		return errors.Annotatef(err, "error creating flow contact")
	}

	// find the topup for this message
	topup, err := models.DecrementOrgCredits(ctx, db, rc, event.OrgID, 1)
	if err != nil {
		return errors.Annotatef(err, "error calculating topup for msg")
	}

	// load the channel for this message
	channel := org.ChannelByID(event.ChannelID)

	// if this channel is no longer active or this contact is blocked, ignore this message (mark it as handled)
	if channel == nil || modelContact.IsBlocked() {
		err := models.UpdateMessage(ctx, db, event.MsgID, models.MsgStatusHandled, models.VisibilityArchived, models.TypeInbox, topup)
		if err != nil {
			return errors.Annotatef(err, "error marking blocked or nil channel message as handled")
		}
		return nil
	}

	// TODO: stopped contact? they are unstopped if they send us an incoming message
	newContact := event.IsNewContact
	if modelContact.IsStopped() {
		err := modelContact.Unstop(ctx, db)
		if err != nil {
			return errors.Annotatef(err, "error unstopping contact")
		}

		newContact = true
	}

	// if this is a new contact, we need to calculate dynamic groups and campaigns
	if newContact {
		// TODO: figure out any dynamic groups the contact is in
		// TODO: figure out any campaigns the contact is in
	}

	// make sure this URN is our highest priority (this is usually a noop)
	err := modelContact.UpdatePreferredURNAndChannel(ctx, db, event.URNID, channel)
	if err != nil {
		return errors.Annotatef(err, "error changing primary URN")
	}

	// find any matching triggers
	trigger := models.FindMatchingMsgTrigger(org, modelContact, event.Text)

	// get any active session for this contact
	session, err := models.ActiveSessionForContact(ctx, db, org, contact)
	if err != nil {
		return errors.Annotatef(err, "error loading active session for contact")
	}

	// we have a session and it has an active flow, check whether we should honor triggers
	var flow *models.Flow
	if session != nil && session.ActiveFlowID != nil {
		assetFlow, err := org.FlowByID(*session.ActiveFlowID)
		if err != nil {
			return errors.Annotatef(err, "error loading flow for session")
		}
		flow = assetFlow.(*models.Flow)
	}

	// we found a trigger and their session is nil or doesn't ignore keywords
	if trigger != nil && (flow == nil || flow.IsArchived() || !flow.IgnoreTriggers()) {
		// TODO: start them in the triggered flow, interrupting their current flow/session
	}

	// if there is a session, resume it
	if flow != nil {
		msg := flows.NewMsgIn(event.MsgUUID, event.MsgID, event.URN, channel, event.Text, event.Attachments)
		runner.ResumeSessionWithMsg()
	}

	// this is a simple message, no session to resume and no trigger, stick it in our inbox
	err := models.UpdateMessage(ctx, db, event.MsgID, models.MsgStatusHandled, models.VisibilityVisible, models.TypeInbox, topup)
	if err != nil {
		return errors.Annotatef(err, "error marking message as handled")
	}
	return nil
}

type msgEvent struct {
	OrgID        models.OrgID        `json:"org_id"`
	ChannelID    models.ChannelID    `json:"channel_id"`
	ContactID    flows.ContactID     `json:"contact_id"`
	MsgID        models.MsgID        `json:"msg_id"`
	MsgUUID      flows.MsgUUID       `json:"msg_uuid"`
	URN          urns.URN            `json:"urn"`
	URNID        models.ContactURNID `json:"urn_id"`
	Text         string              `json:"text"`
	Attachments  []flows.Attachment  `json:"attachments"`
	IsNewContact bool                `json:"is_new_contact"`
}

type handleEventTask struct {
	ContactID flows.ContactID `json:"contact_id"`
}
