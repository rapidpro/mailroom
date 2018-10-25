package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/resumes"
	"github.com/nyaruka/goflow/flows/triggers"
	"github.com/nyaruka/mailroom"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/queue"
	"github.com/nyaruka/mailroom/runner"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// TODO: lock all of these events by contact

const (
	contactEventType         = "handle_event"
	stopEventType            = "stop_event"
	msgEventType             = "msg_event"
	newConversationEventType = "new_conversation"
	referralEventType        = "referral"

	expirationEventType = "expiration_event"
	timeoutEventType    = "timeout_event"
)

func init() {
	mailroom.AddTaskFunction(contactEventType, handleEvent)
}

// AddHandleTask adds a single task for the passed in contact
func AddHandleTask(rc redis.Conn, contactID flows.ContactID, task *queue.Task) error {
	// marshal our task
	taskJSON, err := json.Marshal(task)
	if err != nil {
		return errors.Wrapf(err, "error marshalling contact task")
	}

	// first push the event on our contact queue
	contactQ := fmt.Sprintf("c:%d:%d", task.OrgID, contactID)
	_, err = redis.Int64(rc.Do("rpush", contactQ, string(taskJSON)))
	if err != nil {
		return errors.Wrapf(err, "error adding contact event")
	}

	// create our contact event
	contactTask := &handleEventTask{ContactID: contactID}

	// then add a handle task for that contact
	err = queue.AddTask(rc, mailroom.HandlerQueue, contactEventType, task.OrgID, contactTask, queue.DefaultPriority)
	if err != nil {
		return errors.Wrapf(err, "error adding handle event task")
	}
	return nil
}

func handleEvent(ctx context.Context, mr *mailroom.Mailroom, task *queue.Task) error {
	return handleContactEvent(ctx, mr.DB, mr.RP, task)
}

// handleContactEvent is called when an event comes in for a contact.  to make sure we don't get into
// a situation of being off by one, this task ingests and handles all the events for a contact, one by one
func handleContactEvent(ctx context.Context, db *sqlx.DB, rp *redis.Pool, task *queue.Task) error {
	ctx, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	eventTask := &handleEventTask{}
	err := json.Unmarshal(task.Task, eventTask)
	if err != nil {
		return errors.Wrapf(err, "error decoding contact event task")
	}

	// read all the events for this contact, one by one
	for {
		// pop the next event off this contacts queue
		rc := rp.Get()
		contactQ := fmt.Sprintf("c:%d:%d", task.OrgID, eventTask.ContactID)
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

		// decode our event, this is a normal task at its top level
		contactEvent := &queue.Task{}
		err = json.Unmarshal([]byte(event), contactEvent)
		if err != nil {
			return errors.Wrapf(err, "error unmarshalling contact event")
		}

		// hand off to the appropriate handler
		switch contactEvent.Type {

		case stopEventType:
			evt := &stopEvent{}
			err = json.Unmarshal(contactEvent.Task, evt)
			if err != nil {
				return errors.Wrapf(err, "error unmarshalling stop event")
			}
			err = handleStopEvent(ctx, db, rp, evt)

		case newConversationEventType, referralEventType:
			evt := &channelEvent{}
			err := json.Unmarshal(contactEvent.Task, evt)
			if err != nil {
				return errors.Wrapf(err, "error unmarshalling channel event")
			}
			err = handleChannelEvent(ctx, db, rp, contactEvent.Type, evt)

		case msgEventType:
			msg := &msgEvent{}
			err = json.Unmarshal(contactEvent.Task, msg)
			if err != nil {
				return errors.Wrapf(err, "error unmarshalling msg event")
			}
			err = handleMsgEvent(ctx, db, rp, msg)

		case timeoutEventType, expirationEventType:
			evt := &timedEvent{}
			err = json.Unmarshal(contactEvent.Task, evt)
			if err != nil {
				return errors.Wrapf(err, "error unmarshalling timeout event")
			}
			err = handleTimedEvent(ctx, db, rp, contactEvent.Type, evt)

		default:
			return errors.Errorf("unknown contact event type: %s", contactEvent.Type)
		}

		// if we get an error processing an event, stop processing and return that
		if err != nil {
			return errors.Wrapf(err, "error handling contact event")
		}
	}
}

// handleTimedEvent is called for timeout events
func handleTimedEvent(ctx context.Context, db *sqlx.DB, rp *redis.Pool, eventType string, event *timedEvent) error {
	start := time.Now()
	log := logrus.WithField("event_type", eventType).WithField("contact_id", event.OrgID).WithField("session_id", event.SessionID)
	org, err := models.GetOrgAssets(ctx, db, event.OrgID)
	if err != nil {
		return errors.Wrapf(err, "error loading org")
	}

	// load our contact
	contacts, err := models.LoadContacts(ctx, db, org, []flows.ContactID{event.ContactID})
	if err != nil {
		return errors.Wrapf(err, "error loading contact")
	}

	// contact has been deleted or is blocked, ignore this event
	if len(contacts) == 0 || contacts[0].IsBlocked() {
		return nil
	}

	modelContact := contacts[0]

	// build session assets
	sa, err := models.GetSessionAssets(org)
	if err != nil {
		return errors.Wrapf(err, "unable to load session assets")
	}

	// build our flow contact
	contact, err := modelContact.FlowContact(org, sa)
	if err != nil {
		return errors.Wrapf(err, "error creating flow contact")
	}

	// get the active session for this contact
	session, err := models.ActiveSessionForContact(ctx, db, org, contact)
	if err != nil {
		return errors.Wrapf(err, "error loading active session for contact")
	}

	// if we didn't find a session or it is another session, ignore
	if session == nil || session.ID != event.SessionID {
		log.Info("ignoring event, couldn't find active session")
		return nil
	}

	// resume their flow based on the timeout
	var resume flows.Resume
	switch eventType {
	case expirationEventType:
		// TODO: check our expiration is still the same
		resume = resumes.NewRunExpirationResume(org.Env(), contact)
	case timeoutEventType:
		// TODO: check our timeout is still the same
		// TODO: check that there is no pending outgoing message
		resume = resumes.NewWaitTimeoutResume(org.Env(), contact)
	default:
		return errors.Errorf("unknown event type: %s", eventType)
	}

	_, err = runner.ResumeFlow(ctx, db, rp, org, sa, session, resume, nil)
	if err != nil {
		return errors.Wrapf(err, "error resuming flow for timeout")
	}

	log.WithField("elapsed", time.Since(start)).Info("handled timed event")
	return nil
}

// handleChannelEvent is called for channel events
func handleChannelEvent(ctx context.Context, db *sqlx.DB, rp *redis.Pool, eventType string, event *channelEvent) error {
	org, err := models.GetOrgAssets(ctx, db, event.OrgID)
	if err != nil {
		return errors.Wrapf(err, "error loading org")
	}

	// load the channel for this event
	channel := org.ChannelByID(event.ChannelID)
	if channel == nil {
		return nil
	}

	// do we have associated trigger?
	var trigger *models.Trigger
	switch eventType {
	case newConversationEventType:
		trigger = models.FindMatchingNewConversationTrigger(org, channel)
	case referralEventType:
		trigger = models.FindMatchingReferralTrigger(org, channel, event.Extra["referrer_id"])
	default:
		return errors.Errorf("unknown channel event type: %s", eventType)
	}

	// no trigger, noop, move on
	if trigger == nil {
		return nil
	}

	// ok, we have a trigger, load our contact
	contacts, err := models.LoadContacts(ctx, db, org, []flows.ContactID{event.ContactID})
	if err != nil {
		return errors.Wrapf(err, "error loading contact")
	}

	// contact has been deleted or is blocked, ignore this event
	if len(contacts) == 0 || contacts[0].IsBlocked() {
		return nil
	}

	modelContact := contacts[0]

	// build session assets
	sa, err := models.GetSessionAssets(org)
	if err != nil {
		return errors.Wrapf(err, "unable to load session assets")
	}

	// build our flow contact
	contact, err := modelContact.FlowContact(org, sa)
	if err != nil {
		return errors.Wrapf(err, "error creating flow contact")
	}

	if event.NewContact {
		err = initiliazeNewContact(ctx, db, org, contact)
		if err != nil {
			return errors.Wrapf(err, "unable to initialize new contact")
		}
	}

	// load our flow
	flow, err := org.FlowByID(trigger.FlowID())
	if err != nil {
		return errors.Wrapf(err, "error loading flow for trigger")
	}

	// didn't find it? no longer active, return
	if flow == nil || flow.IsArchived() {
		return nil
	}

	// start them in the triggered flow, interrupting their current flow/session
	// TODO: replace with a real channel trigger
	channelTrigger := triggers.NewManualTrigger(org.Env(), contact, flow.FlowReference(), nil, time.Now())
	_, err = runner.StartFlowForContacts(ctx, db, rp, org, sa, []flows.Trigger{channelTrigger}, nil, true)
	if err != nil {
		return errors.Wrapf(err, "error starting flow for contact")
	}
	return nil
}

// handleStopEvent is called when a contact is stopped by courier
func handleStopEvent(ctx context.Context, db *sqlx.DB, rp *redis.Pool, event *stopEvent) error {
	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		return errors.Wrapf(err, "unable to start transaction for stopping contact")
	}
	err = models.StopContact(ctx, tx, event.OrgID, event.ContactID)
	if err != nil {
		tx.Rollback()
		return err
	}
	err = tx.Commit()
	if err != nil {
		return errors.Wrapf(err, "unable to commit for contact stop")
	}
	return err
}

// handleMsgEvent is called when a new message arrives from a contact
func handleMsgEvent(ctx context.Context, db *sqlx.DB, rp *redis.Pool, event *msgEvent) error {
	org, err := models.GetOrgAssets(ctx, db, event.OrgID)
	if err != nil {
		return errors.Wrapf(err, "error loading org")
	}

	// find the topup for this message
	rc := rp.Get()
	topup, err := models.DecrementOrgCredits(ctx, db, rc, event.OrgID, 1)
	rc.Close()
	if err != nil {
		return errors.Wrapf(err, "error calculating topup for msg")
	}

	// load our contact
	contacts, err := models.LoadContacts(ctx, db, org, []flows.ContactID{event.ContactID})
	if err != nil {
		return errors.Wrapf(err, "error loading contact")
	}

	// contact has been deleted, ignore this message but mark it as handled
	if len(contacts) == 0 {
		err := models.UpdateMessage(ctx, db, event.MsgID, models.MsgStatusHandled, models.VisibilityArchived, models.TypeInbox, topup)
		if err != nil {
			return errors.Wrapf(err, "error updating message for deleted contact")
		}
		return nil
	}

	// build session assets
	sa, err := models.GetSessionAssets(org)
	if err != nil {
		return errors.Wrapf(err, "unable to load session assets")
	}

	modelContact := contacts[0]

	// load the channel for this message
	channel := org.ChannelByID(event.ChannelID)

	// make sure this URN is our highest priority (this is usually a noop)
	err = modelContact.UpdatePreferredURN(ctx, db, org, event.URNID, channel)
	if err != nil {
		return errors.Wrapf(err, "error changing primary URN")
	}

	// build our flow contact
	contact, err := modelContact.FlowContact(org, sa)
	if err != nil {
		return errors.Wrapf(err, "error creating flow contact")
	}

	// if this channel is no longer active or this contact is blocked, ignore this message (mark it as handled)
	if channel == nil || modelContact.IsBlocked() {
		err := models.UpdateMessage(ctx, db, event.MsgID, models.MsgStatusHandled, models.VisibilityArchived, models.TypeInbox, topup)
		if err != nil {
			return errors.Wrapf(err, "error marking blocked or nil channel message as handled")
		}
		return nil
	}

	// stopped contact? they are unstopped if they send us an incoming message
	newContact := event.NewContact
	if modelContact.IsStopped() {
		err := modelContact.Unstop(ctx, db)
		if err != nil {
			return errors.Wrapf(err, "error unstopping contact")
		}

		newContact = true
	}

	// if this is a new contact, we need to calculate dynamic groups and campaigns
	if newContact {
		err = initiliazeNewContact(ctx, db, org, contact)
		if err != nil {
			return errors.Wrapf(err, "unable to initialize new contact")
		}
	}

	// find any matching triggers
	trigger := models.FindMatchingMsgTrigger(org, contact, event.Text)

	// get any active session for this contact
	session, err := models.ActiveSessionForContact(ctx, db, org, contact)
	if err != nil {
		return errors.Wrapf(err, "error loading active session for contact")
	}

	// we have a session and it has an active flow, check whether we should honor triggers
	var flow *models.Flow
	if session != nil && session.CurrentFlowID != nil {
		flow, err = org.FlowByID(*session.CurrentFlowID)
		if err != nil {
			return errors.Wrapf(err, "error loading flow for session")
		}
	}

	msgIn := flows.NewMsgIn(event.MsgUUID, event.MsgID, event.URN, channel.ChannelReference(), event.Text, event.Attachments)

	// build our hook to mark our message as handled
	hook := func(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, sessions []*models.Session) error {
		// set our incoming message event on our session
		if len(sessions) != 1 {
			return errors.Errorf("handle hook called with more than one session")
		}
		sessions[0].SetIncomingMsg(event.MsgID, event.MsgExternalID)

		err = models.UpdateMessage(ctx, tx, event.MsgID, models.MsgStatusHandled, models.VisibilityVisible, models.TypeFlow, topup)
		if err != nil {
			return errors.Wrapf(err, "error marking message as handled")
		}
		return nil
	}

	// we found a trigger and their session is nil or doesn't ignore keywords
	if (trigger != nil && trigger.TriggerType() != models.CatchallTriggerType && (flow == nil || flow.IsArchived() || !flow.IgnoreTriggers())) ||
		(trigger != nil && trigger.TriggerType() == models.CatchallTriggerType && (flow == nil || flow.IsArchived())) {
		// load our flow
		flow, err := org.FlowByID(trigger.FlowID())
		if err != nil {
			return errors.Wrapf(err, "error loading flow for trigger")
		}

		// trigger flow is still active, start it
		if flow != nil && !flow.IsArchived() {
			// start them in the triggered flow, interrupting their current flow/session
			var match *triggers.KeywordMatch

			// if our trigger is on a keyword, populate the type
			if trigger.Keyword() != "" {
				match = &triggers.KeywordMatch{
					Type:    trigger.KeywordMatchType(),
					Keyword: trigger.Keyword(),
				}
			}
			trigger := triggers.NewMsgTrigger(org.Env(), contact, flow.FlowReference(), msgIn, match, time.Now())

			_, err = runner.StartFlowForContacts(ctx, db, rp, org, sa, []flows.Trigger{trigger}, hook, true)
			if err != nil {
				return errors.Wrapf(err, "error starting flow for contact")
			}
			return nil
		}
	}

	// if there is a session, resume it
	if session != nil && flow != nil {
		resume := resumes.NewMsgResume(org.Env(), contact, msgIn)
		_, err = runner.ResumeFlow(ctx, db, rp, org, sa, session, resume, hook)
		if err != nil {
			return errors.Wrapf(err, "error resuming flow for contact")
		}
		return nil
	}

	err = models.UpdateMessage(ctx, db, event.MsgID, models.MsgStatusHandled, models.VisibilityVisible, models.TypeInbox, topup)
	if err != nil {
		return errors.Wrapf(err, "error marking message as handled")
	}
	return nil
}

// initializeNewContact initializes the passed in contact, making sure it is part of any dynamic groups it
// should be as well as taking care of any campaign events.
func initiliazeNewContact(ctx context.Context, db *sqlx.DB, org *models.OrgAssets, contact *flows.Contact) error {
	orgGroups, _ := org.Groups()
	added, removed, errs := contact.ReevaluateDynamicGroups(org.Env(), flows.NewGroupAssets(orgGroups))
	if len(errs) > 0 {
		return errors.Wrapf(errs[0], "error calculating dynamic groups")
	}

	// start a transaction to commit all our changes at once
	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		return errors.Wrapf(err, "unable to start transaction")
	}
	campaigns := make(map[models.CampaignID]*models.Campaign)

	groupAdds := make([]*models.GroupAdd, 0, 1)
	for _, a := range added {
		group := org.GroupByUUID(a.UUID())
		if group == nil {
			return errors.Wrapf(err, "added to unknown group: %s", a.UUID())
		}
		groupAdds = append(groupAdds, &models.GroupAdd{
			ContactID: contact.ID(),
			GroupID:   group.ID(),
		})

		// add in any campaigns we may qualify for
		for _, c := range org.CampaignByGroupID(group.ID()) {
			campaigns[c.ID()] = c
		}
	}
	err = models.AddContactsToGroups(ctx, tx, groupAdds)
	if err != nil {
		return errors.Wrapf(err, "error adding contact to groups")
	}

	groupRemoves := make([]*models.GroupRemove, 0, 1)
	for _, r := range removed {
		group := org.GroupByUUID(r.UUID())
		if group == nil {
			return errors.Wrapf(err, "removed from an unknown group: %s", r.UUID())
		}
		groupRemoves = append(groupRemoves, &models.GroupRemove{
			ContactID: contact.ID(),
			GroupID:   group.ID(),
		})
	}
	err = models.RemoveContactsFromGroups(ctx, tx, groupRemoves)
	if err != nil {
		return errors.Wrapf(err, "error removing contact from group")
	}

	// for each campaign figure out if we need to be added to any events
	fireAdds := make([]*models.FireAdd, 0, 2)
	tz := org.Env().Timezone()
	now := time.Now()
	for _, c := range campaigns {
		for _, ce := range c.Events() {
			scheduled, err := ce.ScheduleForContact(tz, now, contact)
			if err != nil {
				return errors.Wrapf(err, "error calculating schedule for event: %d", ce.ID())
			}

			if scheduled != nil {
				fireAdds = append(fireAdds, &models.FireAdd{
					ContactID: contact.ID(),
					EventID:   ce.ID(),
					Scheduled: *scheduled,
				})
			}
		}
	}

	// add any event adds
	err = models.AddEventFires(ctx, tx, fireAdds)
	if err != nil {
		return errors.Wrapf(err, "unable to add new event fires for contact")
	}

	// ok, commit everything
	err = tx.Commit()
	if err != nil {
		return errors.Wrapf(err, "unable to commit new contact updates")
	}

	return nil
}

type handleEventTask struct {
	ContactID flows.ContactID `json:"contact_id"`
}

type timedEvent struct {
	ContactID flows.ContactID  `json:"contact_id"`
	OrgID     models.OrgID     `json:"org_id"`
	SessionID models.SessionID `json:"session_id"`
	Time      time.Time        `json:"time"`
}

type msgEvent struct {
	ContactID     flows.ContactID    `json:"contact_id"`
	OrgID         models.OrgID       `json:"org_id"`
	ChannelID     models.ChannelID   `json:"channel_id"`
	MsgID         flows.MsgID        `json:"msg_id"`
	MsgUUID       flows.MsgUUID      `json:"msg_uuid"`
	MsgExternalID string             `json:"msg_external_id"`
	URN           urns.URN           `json:"urn"`
	URNID         models.URNID       `json:"urn_id"`
	Text          string             `json:"text"`
	Attachments   []flows.Attachment `json:"attachments"`
	NewContact    bool               `json:"new_contact"`
}

type stopEvent struct {
	ContactID flows.ContactID `json:"contact_id"`
	OrgID     models.OrgID    `json:"org_id"`
}

type channelEvent struct {
	ContactID  flows.ContactID   `json:"contact_id"`
	OrgID      models.OrgID      `json:"org_id"`
	ChannelID  models.ChannelID  `json:"channel_id"`
	Extra      map[string]string `json:"extra"`
	NewContact bool              `json:"new_contact"`
}

// NewTimeoutEvent creates a new event task for the passed in timeout event
func newTimedEvent(eventType string, orgID models.OrgID, contactID flows.ContactID, sessionID models.SessionID, time time.Time) *queue.Task {
	event := &timedEvent{
		OrgID:     orgID,
		ContactID: contactID,
		SessionID: sessionID,
		Time:      time,
	}
	eventJSON, err := json.Marshal(event)
	if err != nil {
		panic(err)
	}

	task := &queue.Task{
		Type:  eventType,
		OrgID: int(orgID),
		Task:  eventJSON,
	}

	return task
}

// NewTimeoutEvent creates a new event task for the passed in timeout event
func NewTimeoutEvent(orgID models.OrgID, contactID flows.ContactID, sessionID models.SessionID, time time.Time) *queue.Task {
	return newTimedEvent(timeoutEventType, orgID, contactID, sessionID, time)
}

// NewExpirationEvent creates a new event task for the passed in expiration event
func NewExpirationEvent(orgID models.OrgID, contactID flows.ContactID, sessionID models.SessionID, time time.Time) *queue.Task {
	return newTimedEvent(expirationEventType, orgID, contactID, sessionID, time)
}
