package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/gocommon/dbutil"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/excellent/types"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/goflow/flows/resumes"
	"github.com/nyaruka/goflow/flows/triggers"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/librato"
	"github.com/nyaruka/mailroom"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/queue"
	"github.com/nyaruka/mailroom/core/runner"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/utils/locker"
	"github.com/nyaruka/null"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	MOMissEventType          = string(models.MOMissEventType)
	NewConversationEventType = "new_conversation"
	WelcomeMessageEventType  = "welcome_message"
	ReferralEventType        = "referral"
	StopEventType            = "stop_event"
	MsgEventType             = "msg_event"
	ExpirationEventType      = "expiration_event"
	TimeoutEventType         = "timeout_event"
	TicketClosedEventType    = "ticket_closed"
)

func init() {
	mailroom.AddTaskFunction(queue.HandleContactEvent, HandleEvent)
}

func HandleEvent(ctx context.Context, rt *runtime.Runtime, task *queue.Task) error {
	return handleContactEvent(ctx, rt, task)
}

// Called when an event comes in for a contact. To make sure we don't get into a situation of being off by one,
// this task ingests and handles all the events for a contact, one by one.
func handleContactEvent(ctx context.Context, rt *runtime.Runtime, task *queue.Task) error {
	ctx, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	eventTask := &HandleEventTask{}
	err := json.Unmarshal(task.Task, eventTask)
	if err != nil {
		return errors.Wrapf(err, "error decoding contact event task")
	}

	// acquire the lock for this contact
	lockID := models.ContactLock(models.OrgID(task.OrgID), eventTask.ContactID)
	lock, err := locker.GrabLock(rt.RP, lockID, time.Minute*5, time.Second*10)
	if err != nil {
		return errors.Wrapf(err, "error acquiring lock for contact %d", eventTask.ContactID)
	}

	// we didn't get the lock within our timeout, skip and requeue for later
	if lock == "" {
		rc := rt.RP.Get()
		defer rc.Close()
		err = queueContactTask(rc, models.OrgID(task.OrgID), eventTask.ContactID)
		if err != nil {
			return errors.Wrapf(err, "error re-adding contact task after failing to get lock")
		}
		logrus.WithFields(logrus.Fields{
			"org_id":     task.OrgID,
			"contact_id": eventTask.ContactID,
		}).Info("failed to get lock for contact, requeued and skipping")
		return nil
	}
	defer locker.ReleaseLock(rt.RP, lockID, lock)

	// read all the events for this contact, one by one
	contactQ := fmt.Sprintf("c:%d:%d", task.OrgID, eventTask.ContactID)
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
		err = json.Unmarshal([]byte(event), contactEvent)
		if err != nil {
			return errors.Wrapf(err, "error unmarshalling contact event: %s", event)
		}

		// hand off to the appropriate handler
		switch contactEvent.Type {

		case StopEventType:
			evt := &StopEvent{}
			err = json.Unmarshal(contactEvent.Task, evt)
			if err != nil {
				return errors.Wrapf(err, "error unmarshalling stop event: %s", event)
			}
			err = handleStopEvent(ctx, rt, evt)

		case NewConversationEventType, ReferralEventType, MOMissEventType, WelcomeMessageEventType:
			evt := &models.ChannelEvent{}
			err = json.Unmarshal(contactEvent.Task, evt)
			if err != nil {
				return errors.Wrapf(err, "error unmarshalling channel event: %s", event)
			}
			_, err = HandleChannelEvent(ctx, rt, models.ChannelEventType(contactEvent.Type), evt, nil)

		case MsgEventType:
			msg := &MsgEvent{}
			err = json.Unmarshal(contactEvent.Task, msg)
			if err != nil {
				return errors.Wrapf(err, "error unmarshalling msg event: %s", event)
			}
			err = handleMsgEvent(ctx, rt, msg)

		case TicketClosedEventType:
			evt := &models.TicketEvent{}
			err = json.Unmarshal(contactEvent.Task, evt)
			if err != nil {
				return errors.Wrapf(err, "error unmarshalling ticket event: %s", event)
			}
			err = handleTicketEvent(ctx, rt, evt)

		case TimeoutEventType, ExpirationEventType:
			evt := &TimedEvent{}
			err = json.Unmarshal(contactEvent.Task, evt)
			if err != nil {
				return errors.Wrapf(err, "error unmarshalling timeout event: %s", event)
			}
			err = handleTimedEvent(ctx, rt, contactEvent.Type, evt)

		default:
			return errors.Errorf("unknown contact event type: %s", contactEvent.Type)
		}

		// log our processing time to librato
		librato.Gauge(fmt.Sprintf("mr.%s_elapsed", contactEvent.Type), float64(time.Since(start))/float64(time.Second))

		// and total latency for this task since it was queued
		librato.Gauge(fmt.Sprintf("mr.%s_latency", contactEvent.Type), float64(time.Since(task.QueuedOn))/float64(time.Second))

		// if we get an error processing an event, requeue it for later and return our error
		if err != nil {
			log := logrus.WithFields(logrus.Fields{
				"org_id":     task.OrgID,
				"contact_id": eventTask.ContactID,
				"event":      event,
			})

			if qerr := dbutil.AsQueryError(err); qerr != nil {
				query, params := qerr.Query()
				log = log.WithFields(logrus.Fields{"sql": query, "sql_params": params})
			}

			contactEvent.ErrorCount++
			if contactEvent.ErrorCount < 3 {
				rc := rt.RP.Get()
				retryErr := queueHandleTask(rc, eventTask.ContactID, contactEvent, true)
				if retryErr != nil {
					logrus.WithError(retryErr).Error("error requeuing errored contact event")
				}
				rc.Close()

				log.WithError(err).WithField("error_count", contactEvent.ErrorCount).Error("error handling contact event")
				return nil
			}
			log.WithError(err).Error("error handling contact event, permanent failure")
			return nil
		}
	}
}

// handleTimedEvent is called for timeout events
func handleTimedEvent(ctx context.Context, rt *runtime.Runtime, eventType string, event *TimedEvent) error {
	start := time.Now()
	log := logrus.WithFields(logrus.Fields{"event_type": eventType, "contact_id": event.ContactID, "session_id": event.SessionID})

	oa, err := models.GetOrgAssets(ctx, rt, event.OrgID)
	if err != nil {
		return errors.Wrapf(err, "error loading org")
	}

	// load our contact
	contacts, err := models.LoadContacts(ctx, rt.ReadonlyDB, oa, []models.ContactID{event.ContactID})
	if err != nil {
		return errors.Wrapf(err, "error loading contact")
	}

	// contact has been deleted or is blocked/stopped/archived, ignore this event
	if len(contacts) == 0 || contacts[0].Status() != models.ContactStatusActive {
		return nil
	}

	modelContact := contacts[0]

	// build our flow contact
	contact, err := modelContact.FlowContact(oa)
	if err != nil {
		return errors.Wrapf(err, "error creating flow contact")
	}

	// look for a waiting session for this contact
	session, err := models.FindWaitingSessionForContact(ctx, rt.DB, rt.SessionStorage, oa, models.FlowTypeMessaging, contact)
	if err != nil {
		return errors.Wrapf(err, "error loading waiting session for contact")
	}

	// if we didn't find a session or it is another session then this session has already been interrupted
	if session == nil || session.ID() != event.SessionID {
		return nil
	}

	// resume their flow based on the timed event
	var resume flows.Resume
	switch eventType {

	case ExpirationEventType:
		// check that our expiration is still the same
		expiresOn, err := models.GetSessionWaitExpiresOn(ctx, rt.DB, event.SessionID)
		if err != nil {
			return errors.Wrapf(err, "unable to load expiration for run")
		}

		if expiresOn == nil {
			log.WithField("event_expiration", event.Time).Info("ignoring expiration, session no longer waiting")
			return nil
		}

		if expiresOn != nil && !expiresOn.Equal(event.Time) {
			log.WithField("event_expiration", event.Time).WithField("run_expiration", expiresOn).Info("ignoring expiration, has been updated")
			return nil
		}

		resume = resumes.NewRunExpiration(oa.Env(), contact)

	case TimeoutEventType:
		if session.WaitTimeoutOn() == nil {
			log.WithField("session_id", session.ID()).Info("ignoring session timeout, has no timeout set")
			return nil
		}

		// check that the timeout is the same
		timeout := *session.WaitTimeoutOn()
		if !timeout.Equal(event.Time) {
			log.WithField("event_timeout", event.Time).WithField("session_timeout", timeout).Info("ignoring timeout, has been updated")
			return nil
		}

		resume = resumes.NewWaitTimeout(oa.Env(), contact)

	default:
		return errors.Errorf("unknown event type: %s", eventType)
	}

	_, err = runner.ResumeFlow(ctx, rt, oa, session, modelContact, resume, nil)
	if err != nil {
		return errors.Wrapf(err, "error resuming flow for timeout")
	}

	log.WithField("elapsed", time.Since(start)).Info("handled timed event")
	return nil
}

// HandleChannelEvent is called for channel events
func HandleChannelEvent(ctx context.Context, rt *runtime.Runtime, eventType models.ChannelEventType, event *models.ChannelEvent, conn *models.ChannelConnection) (*models.Session, error) {
	oa, err := models.GetOrgAssets(ctx, rt, event.OrgID())
	if err != nil {
		return nil, errors.Wrapf(err, "error loading org")
	}

	// load the channel for this event
	channel := oa.ChannelByID(event.ChannelID())
	if channel == nil {
		logrus.WithField("channel_id", event.ChannelID).Info("ignoring event, couldn't find channel")
		return nil, nil
	}

	// load our contact
	contacts, err := models.LoadContacts(ctx, rt.ReadonlyDB, oa, []models.ContactID{event.ContactID()})
	if err != nil {
		return nil, errors.Wrapf(err, "error loading contact")
	}

	// contact has been deleted or is blocked, ignore this event
	if len(contacts) == 0 || contacts[0].Status() == models.ContactStatusBlocked {
		return nil, nil
	}

	modelContact := contacts[0]

	if models.ContactSeenEvents[eventType] {
		err = modelContact.UpdateLastSeenOn(ctx, rt.DB, event.OccurredOn())
		if err != nil {
			return nil, errors.Wrap(err, "error updating contact last_seen_on")
		}
	}

	// make sure this URN is our highest priority (this is usually a noop)
	err = modelContact.UpdatePreferredURN(ctx, rt.DB, oa, event.URNID(), channel)
	if err != nil {
		return nil, errors.Wrapf(err, "error changing primary URN")
	}

	// build our flow contact
	contact, err := modelContact.FlowContact(oa)
	if err != nil {
		return nil, errors.Wrapf(err, "error creating flow contact")
	}

	// do we have associated trigger?
	var trigger *models.Trigger

	switch eventType {

	case models.NewConversationEventType:
		trigger = models.FindMatchingNewConversationTrigger(oa, channel)

	case models.ReferralEventType:
		trigger = models.FindMatchingReferralTrigger(oa, channel, event.ExtraValue("referrer_id"))

	case models.MOMissEventType:
		trigger = models.FindMatchingMissedCallTrigger(oa)

	case models.MOCallEventType:
		trigger = models.FindMatchingIncomingCallTrigger(oa, contact)

	case models.WelcomeMessageEventType:
		trigger = nil

	default:
		return nil, errors.Errorf("unknown channel event type: %s", eventType)
	}

	if event.IsNewContact() {
		err = models.CalculateDynamicGroups(ctx, rt.DB, oa, []*flows.Contact{contact})
		if err != nil {
			return nil, errors.Wrapf(err, "unable to initialize new contact")
		}
	}

	// no trigger, noop, move on
	if trigger == nil {
		logrus.WithField("channel_id", event.ChannelID()).WithField("event_type", eventType).WithField("extra", event.Extra()).Info("ignoring channel event, no trigger found")
		return nil, nil
	}

	// load our flow
	flow, err := oa.FlowByID(trigger.FlowID())
	if err == models.ErrNotFound {
		return nil, nil
	}

	if err != nil {
		return nil, errors.Wrapf(err, "error loading flow for trigger")
	}

	// if this is an IVR flow, we need to trigger that start (which happens in a different queue)
	if flow.FlowType() == models.FlowTypeVoice && conn == nil {
		err = runner.TriggerIVRFlow(ctx, rt, oa.OrgID(), flow.ID(), []models.ContactID{modelContact.ID()}, nil)
		if err != nil {
			return nil, errors.Wrapf(err, "error while triggering ivr flow")
		}
		return nil, nil
	}

	// create our parameters, we just convert this from JSON
	var params *types.XObject
	if event.Extra() != nil {
		asJSON, err := json.Marshal(event.Extra())
		if err != nil {
			return nil, errors.Wrapf(err, "unable to marshal extra from channel event")
		}
		params, err = types.ReadXObject(asJSON)
		if err != nil {
			return nil, errors.Wrapf(err, "unable to read extra from channel event")
		}
	}

	// build our flow trigger
	var flowTrigger flows.Trigger
	switch eventType {

	case models.NewConversationEventType, models.ReferralEventType, models.MOMissEventType:
		flowTrigger = triggers.NewBuilder(oa.Env(), flow.Reference(), contact).
			Channel(channel.ChannelReference(), triggers.ChannelEventType(eventType)).
			WithParams(params).
			Build()

	case models.MOCallEventType:
		urn := contacts[0].URNForID(event.URNID())
		flowTrigger = triggers.NewBuilder(oa.Env(), flow.Reference(), contact).
			Channel(channel.ChannelReference(), triggers.ChannelEventTypeIncomingCall).
			WithConnection(urn).
			Build()

	default:
		return nil, errors.Errorf("unknown channel event type: %s", eventType)
	}

	// if we have a channel connection we set the connection on the session before our event hooks fire
	// so that IVR messages can be created with the right connection reference
	var hook models.SessionCommitHook
	if conn != nil {
		hook = func(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, oa *models.OrgAssets, sessions []*models.Session) error {
			for _, session := range sessions {
				session.SetChannelConnection(conn)
			}
			return nil
		}
	}

	sessions, err := runner.StartFlowForContacts(ctx, rt, oa, flow, []*models.Contact{modelContact}, []flows.Trigger{flowTrigger}, hook, true)
	if err != nil {
		return nil, errors.Wrapf(err, "error starting flow for contact")
	}
	if len(sessions) == 0 {
		return nil, nil
	}
	return sessions[0], nil
}

// handleStopEvent is called when a contact is stopped by courier
func handleStopEvent(ctx context.Context, rt *runtime.Runtime, event *StopEvent) error {
	tx, err := rt.DB.BeginTxx(ctx, nil)
	if err != nil {
		return errors.Wrapf(err, "unable to start transaction for stopping contact")
	}

	err = models.StopContact(ctx, tx, event.OrgID, event.ContactID)
	if err != nil {
		tx.Rollback()
		return err
	}

	err = models.UpdateContactLastSeenOn(ctx, tx, event.ContactID, event.OccurredOn)
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
func handleMsgEvent(ctx context.Context, rt *runtime.Runtime, event *MsgEvent) error {
	oa, err := models.GetOrgAssets(ctx, rt, event.OrgID)
	if err != nil {
		return errors.Wrapf(err, "error loading org")
	}

	// allocate a topup for this message if org uses topups
	topupID, err := models.AllocateTopups(ctx, rt.DB, rt.RP, oa.Org(), 1)
	if err != nil {
		return errors.Wrapf(err, "error allocating topup for incoming message")
	}

	// load our contact
	contacts, err := models.LoadContacts(ctx, rt.ReadonlyDB, oa, []models.ContactID{event.ContactID})
	if err != nil {
		return errors.Wrapf(err, "error loading contact")
	}

	// contact has been deleted, ignore this message but mark it as handled
	if len(contacts) == 0 {
		err := models.UpdateMessage(ctx, rt.DB, event.MsgID, models.MsgStatusHandled, models.VisibilityArchived, models.MsgTypeInbox, models.NilFlowID, topupID)
		if err != nil {
			return errors.Wrapf(err, "error updating message for deleted contact")
		}
		return nil
	}

	modelContact := contacts[0]

	// load the channel for this message
	channel := oa.ChannelByID(event.ChannelID)

	// if we have URNs make sure the message URN is our highest priority (this is usually a noop)
	if len(modelContact.URNs()) > 0 {
		err = modelContact.UpdatePreferredURN(ctx, rt.DB, oa, event.URNID, channel)
		if err != nil {
			return errors.Wrapf(err, "error changing primary URN")
		}
	}

	// build our flow contact
	contact, err := modelContact.FlowContact(oa)
	if err != nil {
		return errors.Wrapf(err, "error creating flow contact")
	}

	// if this channel is no longer active or this contact is blocked, ignore this message (mark it as handled)
	if channel == nil || modelContact.Status() == models.ContactStatusBlocked {
		err := models.UpdateMessage(ctx, rt.DB, event.MsgID, models.MsgStatusHandled, models.VisibilityArchived, models.MsgTypeInbox, models.NilFlowID, topupID)
		if err != nil {
			return errors.Wrapf(err, "error marking blocked or nil channel message as handled")
		}
		return nil
	}

	// stopped contact? they are unstopped if they send us an incoming message
	newContact := event.NewContact
	if modelContact.Status() == models.ContactStatusStopped {
		err := modelContact.Unstop(ctx, rt.DB)
		if err != nil {
			return errors.Wrapf(err, "error unstopping contact")
		}

		newContact = true
	}

	// if this is a new contact, we need to calculate dynamic groups and campaigns
	if newContact {
		err = models.CalculateDynamicGroups(ctx, rt.DB, oa, []*flows.Contact{contact})
		if err != nil {
			return errors.Wrapf(err, "unable to initialize new contact")
		}
	}

	// look up any open tickets for this contact and forward this message to them
	tickets, err := models.LoadOpenTicketsForContact(ctx, rt.DB, modelContact)
	if err != nil {
		return errors.Wrapf(err, "unable to look up open tickets for contact")
	}
	for _, ticket := range tickets {
		ticket.ForwardIncoming(ctx, rt, oa, event.MsgUUID, event.Text, event.Attachments)
	}

	// find any matching triggers
	trigger := models.FindMatchingMsgTrigger(oa, contact, event.Text)

	// look for a waiting session for this contact
	session, err := models.FindWaitingSessionForContact(ctx, rt.DB, rt.SessionStorage, oa, models.FlowTypeMessaging, contact)
	if err != nil {
		return errors.Wrapf(err, "error loading active session for contact")
	}

	// we have a session and it has an active flow, check whether we should honor triggers
	var flow *models.Flow
	if session != nil && session.CurrentFlowID() != models.NilFlowID {
		flow, err = oa.FlowByID(session.CurrentFlowID())

		// flow this session is in is gone, interrupt our session and reset it
		if err == models.ErrNotFound {
			err = models.ExitSessions(ctx, rt.DB, []models.SessionID{session.ID()}, models.SessionStatusFailed)
			session = nil
		}

		if err != nil {
			return errors.Wrapf(err, "error loading flow for session")
		}
	}

	msgIn := flows.NewMsgIn(event.MsgUUID, event.URN, channel.ChannelReference(), event.Text, event.Attachments)
	msgIn.SetExternalID(string(event.MsgExternalID))
	msgIn.SetID(event.MsgID)

	// build our hook to mark a flow message as handled
	flowMsgHook := func(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, oa *models.OrgAssets, sessions []*models.Session) error {
		// set our incoming message event on our session
		if len(sessions) != 1 {
			return errors.Errorf("handle hook called with more than one session")
		}
		sessions[0].SetIncomingMsg(event.MsgID, event.MsgExternalID)

		return markMsgHandled(ctx, tx, contact, msgIn, flow, topupID, tickets)
	}

	// we found a trigger and their session is nil or doesn't ignore keywords
	if (trigger != nil && trigger.TriggerType() != models.CatchallTriggerType && (flow == nil || !flow.IgnoreTriggers())) ||
		(trigger != nil && trigger.TriggerType() == models.CatchallTriggerType && (flow == nil)) {
		// load our flow
		flow, err = oa.FlowByID(trigger.FlowID())
		if err != nil && err != models.ErrNotFound {
			return errors.Wrapf(err, "error loading flow for trigger")
		}

		// trigger flow is still active, start it
		if flow != nil {
			// if this is an IVR flow, we need to trigger that start (which happens in a different queue)
			if flow.FlowType() == models.FlowTypeVoice {
				ivrMsgHook := func(ctx context.Context, tx *sqlx.Tx) error {
					return markMsgHandled(ctx, tx, contact, msgIn, flow, topupID, tickets)
				}
				err = runner.TriggerIVRFlow(ctx, rt, oa.OrgID(), flow.ID(), []models.ContactID{modelContact.ID()}, ivrMsgHook)
				if err != nil {
					return errors.Wrapf(err, "error while triggering ivr flow")
				}
				return nil
			}

			// otherwise build the trigger and start the flow directly
			trigger := triggers.NewBuilder(oa.Env(), flow.Reference(), contact).Msg(msgIn).WithMatch(trigger.Match()).Build()
			_, err = runner.StartFlowForContacts(ctx, rt, oa, flow, []*models.Contact{modelContact}, []flows.Trigger{trigger}, flowMsgHook, true)
			if err != nil {
				return errors.Wrapf(err, "error starting flow for contact")
			}
			return nil
		}
	}

	// if there is a session, resume it
	if session != nil && flow != nil {
		resume := resumes.NewMsg(oa.Env(), contact, msgIn)
		_, err = runner.ResumeFlow(ctx, rt, oa, session, modelContact, resume, flowMsgHook)
		if err != nil {
			return errors.Wrapf(err, "error resuming flow for contact")
		}
		return nil
	}

	// this message didn't trigger and new sessions or resume any existing ones, so handle as inbox
	err = handleAsInbox(ctx, rt, oa, contact, msgIn, topupID, tickets)
	if err != nil {
		return errors.Wrapf(err, "error handling inbox message")
	}
	return nil
}

func handleTicketEvent(ctx context.Context, rt *runtime.Runtime, event *models.TicketEvent) error {
	oa, err := models.GetOrgAssets(ctx, rt, event.OrgID())
	if err != nil {
		return errors.Wrapf(err, "error loading org")
	}

	// load our ticket
	tickets, err := models.LoadTickets(ctx, rt.DB, []models.TicketID{event.TicketID()})
	if err != nil {
		return errors.Wrapf(err, "error loading ticket")
	}
	// ticket has been deleted ignore this event
	if len(tickets) == 0 {
		return nil
	}

	modelTicket := tickets[0]

	// load our contact
	contacts, err := models.LoadContacts(ctx, rt.ReadonlyDB, oa, []models.ContactID{modelTicket.ContactID()})
	if err != nil {
		return errors.Wrapf(err, "error loading contact")
	}

	// contact has been deleted ignore this event
	if len(contacts) == 0 {
		return nil
	}

	modelContact := contacts[0]

	// build our flow contact
	contact, err := modelContact.FlowContact(oa)
	if err != nil {
		return errors.Wrapf(err, "error creating flow contact")
	}

	// do we have associated trigger?
	var trigger *models.Trigger

	switch event.EventType() {
	case models.TicketEventTypeClosed:
		trigger = models.FindMatchingTicketClosedTrigger(oa, contact)
	default:
		return errors.Errorf("unknown ticket event type: %s", event.EventType())
	}

	// no trigger, noop, move on
	if trigger == nil {
		logrus.WithField("ticket_id", event.TicketID).WithField("event_type", event.EventType()).Info("ignoring ticket event, no trigger found")
		return nil
	}

	// load our flow
	flow, err := oa.FlowByID(trigger.FlowID())
	if err == models.ErrNotFound {
		return nil
	}
	if err != nil {
		return errors.Wrapf(err, "error loading flow for trigger")
	}

	// if this is an IVR flow, we need to trigger that start (which happens in a different queue)
	if flow.FlowType() == models.FlowTypeVoice {
		err = runner.TriggerIVRFlow(ctx, rt, oa.OrgID(), flow.ID(), []models.ContactID{modelContact.ID()}, nil)
		if err != nil {
			return errors.Wrapf(err, "error while triggering ivr flow")
		}
		return nil
	}

	// build our flow ticket
	ticket, err := tickets[0].FlowTicket(oa)
	if err != nil {
		return errors.Wrapf(err, "error creating flow contact")
	}

	// build our flow trigger
	var flowTrigger flows.Trigger

	switch event.EventType() {
	case models.TicketEventTypeClosed:
		flowTrigger = triggers.NewBuilder(oa.Env(), flow.Reference(), contact).
			Ticket(ticket, triggers.TicketEventTypeClosed).
			Build()
	default:
		return errors.Errorf("unknown ticket event type: %s", event.EventType())
	}

	_, err = runner.StartFlowForContacts(ctx, rt, oa, flow, []*models.Contact{modelContact}, []flows.Trigger{flowTrigger}, nil, true)
	if err != nil {
		return errors.Wrapf(err, "error starting flow for contact")
	}
	return nil
}

// handles a message as an inbox message
func handleAsInbox(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, contact *flows.Contact, msg *flows.MsgIn, topupID models.TopupID, tickets []*models.Ticket) error {
	// usually last_seen_on is updated by handling the msg_received event in the engine sprint, but since this is an inbox
	// message we manually create that event and handle it
	msgEvent := events.NewMsgReceived(msg)
	contact.SetLastSeenOn(msgEvent.CreatedOn())
	contactEvents := map[*flows.Contact][]flows.Event{contact: {msgEvent}}

	err := models.HandleAndCommitEvents(ctx, rt, oa, contactEvents)
	if err != nil {
		return errors.Wrap(err, "error handling inbox message events")
	}

	return markMsgHandled(ctx, rt.DB, contact, msg, nil, topupID, tickets)
}

// utility to mark as message as handled and update any open contact tickets
func markMsgHandled(ctx context.Context, db models.Queryer, contact *flows.Contact, msg *flows.MsgIn, flow *models.Flow, topupID models.TopupID, tickets []*models.Ticket) error {
	msgType := models.MsgTypeInbox
	flowID := models.NilFlowID
	if flow != nil {
		msgType = models.MsgTypeFlow
		flowID = flow.ID()
	}

	err := models.UpdateMessage(ctx, db, msg.ID(), models.MsgStatusHandled, models.VisibilityVisible, msgType, flowID, topupID)
	if err != nil {
		return errors.Wrapf(err, "error marking message as handled")
	}

	if len(tickets) > 0 {
		err = models.UpdateTicketLastActivity(ctx, db, tickets)
		if err != nil {
			return errors.Wrapf(err, "error updating last activity for open tickets")
		}
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
	Time      time.Time        `json:"time"`
}

type MsgEvent struct {
	ContactID     models.ContactID   `json:"contact_id"`
	OrgID         models.OrgID       `json:"org_id"`
	ChannelID     models.ChannelID   `json:"channel_id"`
	MsgID         flows.MsgID        `json:"msg_id"`
	MsgUUID       flows.MsgUUID      `json:"msg_uuid"`
	MsgExternalID null.String        `json:"msg_external_id"`
	URN           urns.URN           `json:"urn"`
	URNID         models.URNID       `json:"urn_id"`
	Text          string             `json:"text"`
	Attachments   []utils.Attachment `json:"attachments"`
	NewContact    bool               `json:"new_contact"`
	CreatedOn     time.Time          `json:"created_on"`
}

type StopEvent struct {
	ContactID  models.ContactID `json:"contact_id"`
	OrgID      models.OrgID     `json:"org_id"`
	OccurredOn time.Time        `json:"occurred_on"`
}

// creates a new event task for the passed in timeout event
func newTimedTask(eventType string, orgID models.OrgID, contactID models.ContactID, sessionID models.SessionID, eventTime time.Time) *queue.Task {
	event := &TimedEvent{
		OrgID:     orgID,
		ContactID: contactID,
		SessionID: sessionID,
		Time:      eventTime,
	}
	eventJSON, err := json.Marshal(event)
	if err != nil {
		panic(err)
	}

	task := &queue.Task{
		Type:     eventType,
		OrgID:    int(orgID),
		Task:     eventJSON,
		QueuedOn: time.Now(),
	}

	return task
}

// NewTimeoutTask creates a new event task for the passed in timeout event
func NewTimeoutTask(orgID models.OrgID, contactID models.ContactID, sessionID models.SessionID, time time.Time) *queue.Task {
	return newTimedTask(TimeoutEventType, orgID, contactID, sessionID, time)
}

// NewExpirationTask creates a new event task for the passed in expiration event
func NewExpirationTask(orgID models.OrgID, contactID models.ContactID, sessionID models.SessionID, time time.Time) *queue.Task {
	return newTimedTask(ExpirationEventType, orgID, contactID, sessionID, time)
}
