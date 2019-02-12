package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/excellent/types"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/resumes"
	"github.com/nyaruka/goflow/flows/triggers"
	"github.com/nyaruka/mailroom"
	"github.com/nyaruka/mailroom/locker"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/queue"
	"github.com/nyaruka/mailroom/runner"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	NewConversationEventType = "new_conversation"
	ReferralEventType        = "referral"
	StopEventType            = "stop_event"
	MsgEventType             = "msg_event"
	ExpirationEventType      = "expiration_event"
	TimeoutEventType         = "timeout_event"
)

func init() {
	mailroom.AddTaskFunction(mailroom.HandleContactEvent, handleEvent)
}

// AddHandleTask adds a single task for the passed in contact
func AddHandleTask(rc redis.Conn, contactID models.ContactID, task *queue.Task) error {
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
	contactTask := &HandleEventTask{ContactID: contactID}

	// then add a handle task for that contact
	err = queue.AddTask(rc, mailroom.HandlerQueue, mailroom.HandleContactEvent, task.OrgID, contactTask, queue.DefaultPriority)
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

	eventTask := &HandleEventTask{}
	err := json.Unmarshal(task.Task, eventTask)
	if err != nil {
		return errors.Wrapf(err, "error decoding contact event task")
	}

	// acquire the lock for this contact
	lockID := models.ContactLock(models.OrgID(task.OrgID), eventTask.ContactID)
	lock, err := locker.GrabLock(rp, lockID, time.Minute*5, time.Minute*5)
	if err != nil {
		return errors.Wrapf(err, "error acquiring lock for contact %d", eventTask.ContactID)
	}
	if lock == "" {
		return errors.Errorf("unable to acquire lock for contact %d in timeout period, skipping", eventTask.ContactID)
	}
	defer locker.ReleaseLock(rp, lockID, lock)

	// read all the events for this contact, one by one
	contactQ := fmt.Sprintf("c:%d:%d", task.OrgID, eventTask.ContactID)
	for {
		// pop the next event off this contacts queue
		rc := rp.Get()
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

		case StopEventType:
			evt := &StopEvent{}
			err = json.Unmarshal(contactEvent.Task, evt)
			if err != nil {
				return errors.Wrapf(err, "error unmarshalling stop event")
			}
			err = handleStopEvent(ctx, db, rp, evt)

		case NewConversationEventType, ReferralEventType:
			evt := &models.ChannelEvent{}
			err = json.Unmarshal(contactEvent.Task, evt)
			if err != nil {
				return errors.Wrapf(err, "error unmarshalling channel event")
			}
			// TODO: we should just have courier include event type in its json
			_, err = HandleChannelEvent(ctx, db, rp, models.ChannelEventType(contactEvent.Type), evt, nil)

		case MsgEventType:
			msg := &MsgEvent{}
			err = json.Unmarshal(contactEvent.Task, msg)
			if err != nil {
				return errors.Wrapf(err, "error unmarshalling msg event")
			}
			err = handleMsgEvent(ctx, db, rp, msg)

		case TimeoutEventType, ExpirationEventType:
			evt := &TimedEvent{}
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
func handleTimedEvent(ctx context.Context, db *sqlx.DB, rp *redis.Pool, eventType string, event *TimedEvent) error {
	start := time.Now()
	log := logrus.WithField("event_type", eventType).WithField("contact_id", event.OrgID).WithField("session_id", event.SessionID)
	org, err := models.GetOrgAssets(ctx, db, event.OrgID)
	if err != nil {
		return errors.Wrapf(err, "error loading org")
	}

	// load our contact
	contacts, err := models.LoadContacts(ctx, db, org, []models.ContactID{event.ContactID})
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
	if session == nil || session.ID() != event.SessionID {
		log.Info("ignoring event, couldn't find active session")
		return nil
	}

	// resume their flow based on the timed event
	var resume flows.Resume
	switch eventType {
	case ExpirationEventType:
		// check that our expiration is still the same
		expiration, err := models.RunExpiration(ctx, db, event.RunID)
		if err != nil {
			return errors.Wrapf(err, "unable to load expiration for run")
		}

		if !expiration.Equal(event.Time) {
			log.WithField("event_expiration", event.Time).WithField("run_expiration", expiration).Info("ignoring expiration, has been updated")
			return nil
		}

		resume = resumes.NewRunExpirationResume(org.Env(), contact)
	case TimeoutEventType:
		if session.TimeoutOn() == nil {
			log.WithField("session_id", session.ID).Info("ignoring session timeout, has no timeout set")
			return nil
		}

		// check that the timeout is the same
		timeout := *session.TimeoutOn()
		if !timeout.Equal(event.Time) {
			log.WithField("event_timeout", event.Time).WithField("session_timeout", timeout).Info("ignoring timeout, has been updated")
			return nil
		}

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

// HandleChannelEvent is called for channel events
func HandleChannelEvent(ctx context.Context, db *sqlx.DB, rp *redis.Pool, eventType models.ChannelEventType, event *models.ChannelEvent, hook models.SessionCommitHook) (*models.Session, error) {
	org, err := models.GetOrgAssets(ctx, db, event.OrgID())
	if err != nil {
		return nil, errors.Wrapf(err, "error loading org")
	}

	// load the channel for this event
	channel := org.ChannelByID(event.ChannelID())
	if channel == nil {
		logrus.WithField("channel_id", event.ChannelID).Info("ignoring event, couldn't find channel")
		return nil, nil
	}

	// load our contact
	contacts, err := models.LoadContacts(ctx, db, org, []models.ContactID{event.ContactID()})
	if err != nil {
		return nil, errors.Wrapf(err, "error loading contact")
	}

	// contact has been deleted or is blocked, ignore this event
	if len(contacts) == 0 || contacts[0].IsBlocked() {
		return nil, nil
	}

	modelContact := contacts[0]

	// do we have associated trigger?
	var trigger *models.Trigger
	switch eventType {

	case models.NewConversationEventType:
		trigger = models.FindMatchingNewConversationTrigger(org, channel)

	case models.ReferralEventType:
		trigger = models.FindMatchingReferralTrigger(org, channel, event.Extra()["referrer_id"])

	case models.MOMissEventType:
		trigger = models.FindMatchingMissedCallTrigger(org)

	case models.MOCallEventType:
		trigger = models.FindMatchingMOCallTrigger(org, modelContact)

	default:
		return nil, errors.Errorf("unknown channel event type: %s", eventType)
	}

	// no trigger, noop, move on
	if trigger == nil {
		logrus.WithField("channel_id", event.ChannelID()).WithField("event_type", eventType).Info("ignoring event, no trigger found")
		return nil, nil
	}

	// build session assets
	sa, err := models.GetSessionAssets(org)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to load session assets")
	}

	// make sure this URN is our highest priority (this is usually a noop)
	err = modelContact.UpdatePreferredURN(ctx, db, org, event.URNID(), channel)
	if err != nil {
		return nil, errors.Wrapf(err, "error changing primary URN")
	}

	// build our flow contact
	contact, err := modelContact.FlowContact(org, sa)
	if err != nil {
		return nil, errors.Wrapf(err, "error creating flow contact")
	}

	if event.IsNewContact() {
		err = models.CalculateDynamicGroups(ctx, db, org, contact)
		if err != nil {
			return nil, errors.Wrapf(err, "unable to initialize new contact")
		}
	}

	// load our flow
	flow, err := org.FlowByID(trigger.FlowID())
	if err != nil {
		return nil, errors.Wrapf(err, "error loading flow for trigger")
	}

	// didn't find it? no longer active, return
	if flow == nil || flow.IsArchived() {
		return nil, nil
	}

	// create our parameters, we just convert this from JSON
	// TODO: this is done because a nil XJSONObject doesn't know how to marshal itself, goflow could fix
	params := types.NewXJSONObject([]byte("{}"))
	if event.Extra() != nil {
		asJSON, err := json.Marshal(event.Extra())
		if err != nil {
			return nil, errors.Wrapf(err, "unable to marshal extra from channel event")
		}
		params = types.NewXJSONObject(asJSON)
	}

	// build our flow trigger
	var flowTrigger flows.Trigger
	switch eventType {

	case models.NewConversationEventType, models.ReferralEventType, models.MOMissEventType:
		channelEvent := triggers.NewChannelEvent(triggers.ChannelEventType(eventType), channel.ChannelReference())
		flowTrigger = triggers.NewChannelTrigger(org.Env(), flow.FlowReference(), contact, channelEvent, params)

	case models.MOCallEventType:
		urn := contacts[0].URNForID(event.URNID())
		flowTrigger = triggers.NewIncomingCallTrigger(org.Env(), flow.FlowReference(), contact, urn, channel.ChannelReference())

	default:
		return nil, errors.Errorf("unknown channel event type: %s", eventType)
	}

	sessions, err := runner.StartFlowForContacts(ctx, db, rp, org, sa, []flows.Trigger{flowTrigger}, hook, true)
	if err != nil {
		return nil, errors.Wrapf(err, "error starting flow for contact")
	}
	if len(sessions) == 0 {
		return nil, nil
	}
	return sessions[0], nil
}

// handleStopEvent is called when a contact is stopped by courier
func handleStopEvent(ctx context.Context, db *sqlx.DB, rp *redis.Pool, event *StopEvent) error {
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
func handleMsgEvent(ctx context.Context, db *sqlx.DB, rp *redis.Pool, event *MsgEvent) error {
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
	contacts, err := models.LoadContacts(ctx, db, org, []models.ContactID{event.ContactID})
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
		err = models.CalculateDynamicGroups(ctx, db, org, contact)
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
	if session != nil && session.CurrentFlowID() != nil {
		flow, err = org.FlowByID(*session.CurrentFlowID())
		if err != nil {
			return errors.Wrapf(err, "error loading flow for session")
		}
	}

	msgIn := flows.NewMsgIn(event.MsgUUID, event.URN, channel.ChannelReference(), event.Text, event.Attachments)
	msgIn.SetExternalID(event.MsgExternalID)
	msgIn.SetID(event.MsgID)

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
			trigger := triggers.NewMsgTrigger(org.Env(), flow.FlowReference(), contact, msgIn, match)

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

type HandleEventTask struct {
	ContactID models.ContactID `json:"contact_id"`
}

type TimedEvent struct {
	ContactID models.ContactID `json:"contact_id"`
	OrgID     models.OrgID     `json:"org_id"`
	SessionID models.SessionID `json:"session_id"`
	RunID     models.FlowRunID `json:"run_id,omitempty"`
	Time      time.Time        `json:"time"`
}

type MsgEvent struct {
	ContactID     models.ContactID   `json:"contact_id"`
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

type StopEvent struct {
	ContactID models.ContactID `json:"contact_id"`
	OrgID     models.OrgID     `json:"org_id"`
}

// NewTimeoutEvent creates a new event task for the passed in timeout event
func newTimedTask(eventType string, orgID models.OrgID, contactID models.ContactID, sessionID models.SessionID, runID models.FlowRunID, time time.Time) *queue.Task {
	event := &TimedEvent{
		OrgID:     orgID,
		ContactID: contactID,
		SessionID: sessionID,
		RunID:     runID,
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

// NewTimeoutTask creates a new event task for the passed in timeout event
func NewTimeoutTask(orgID models.OrgID, contactID models.ContactID, sessionID models.SessionID, time time.Time) *queue.Task {
	return newTimedTask(TimeoutEventType, orgID, contactID, sessionID, models.NilFlowRunID, time)
}

// NewExpirationTask creates a new event task for the passed in expiration event
func NewExpirationTask(orgID models.OrgID, contactID models.ContactID, sessionID models.SessionID, runID models.FlowRunID, time time.Time) *queue.Task {
	return newTimedTask(ExpirationEventType, orgID, contactID, sessionID, runID, time)
}
