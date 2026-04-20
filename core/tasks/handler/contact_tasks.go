package handler

import (
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/excellent/types"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/engine"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/goflow/flows/resumes"
	"github.com/nyaruka/goflow/flows/triggers"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/msgio"
	"github.com/nyaruka/mailroom/core/queue"
	"github.com/nyaruka/mailroom/core/runner"
	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/nyaruka/mailroom/core/tasks/ivr"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/null/v3"
	"github.com/pkg/errors"
)

const (
	MsgEventType          = "msg_event"
	ExpirationEventType   = "expiration_event"
	TimeoutEventType      = "timeout_event"
	TicketClosedEventType = "ticket_closed"
	MsgDeletedType        = "msg_deleted"
)

// handleTimedEvent is called for timeout events
func handleTimedEvent(ctx context.Context, rt *runtime.Runtime, eventType string, event *TimedEvent) error {
	start := time.Now()
	log := slog.With("event_type", eventType, "contact_id", event.ContactID, "session_id", event.SessionID)

	oa, err := models.GetOrgAssets(ctx, rt, event.OrgID)
	if err != nil {
		return errors.Wrapf(err, "error loading org")
	}

	// load our contact
	modelContact, err := models.LoadContact(ctx, rt.ReadonlyDB, oa, event.ContactID)
	if err != nil {
		if err == sql.ErrNoRows { // if contact no longer exists, ignore event
			return nil
		}
		return errors.Wrapf(err, "error loading contact")
	}

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
			log.Info("ignoring expiration, session no longer waiting", "event_expiration", event.Time)
			return nil
		}

		if expiresOn != nil && !expiresOn.Equal(event.Time) {
			log.Info("ignoring expiration, has been updated", "event_expiration", event.Time, "run_expiration", expiresOn)
			return nil
		}

		resume = resumes.NewRunExpiration(oa.Env(), contact)

	case TimeoutEventType:
		if session.WaitTimeoutOn() == nil {
			log.Info("ignoring session timeout, has no timeout set", "session_id", session.ID())
			return nil
		}

		// check that the timeout is the same
		timeout := *session.WaitTimeoutOn()
		if !timeout.Equal(event.Time) {
			log.Info("ignoring timeout, has been updated", "event_timeout", event.Time, "session_timeout", timeout)
			return nil
		}

		resume = resumes.NewWaitTimeout(oa.Env(), contact)

	default:
		return errors.Errorf("unknown event type: %s", eventType)
	}

	_, err = runner.ResumeFlow(ctx, rt, oa, session, modelContact, resume, nil)
	if err != nil {
		// if we errored, and it's the wait rejecting the timeout event, it's because it no longer exists on the flow, so clear it
		// on the session
		var eerr *engine.Error
		if errors.As(err, &eerr) && eerr.Code() == engine.ErrorResumeRejectedByWait && resume.Type() == resumes.TypeWaitTimeout {
			log.Info("clearing session timeout which is no longer set in flow", "session_id", session.ID())
			return errors.Wrap(session.ClearWaitTimeout(ctx, rt.DB), "error clearing session timeout")
		}

		return errors.Wrap(err, "error resuming flow for timeout")
	}

	log.Info("handled timed event", "elapsed", time.Since(start))
	return nil
}

// HandleChannelEvent is called for channel events
func HandleChannelEvent(ctx context.Context, rt *runtime.Runtime, eventType models.ChannelEventType, event *models.ChannelEvent, call *models.Call) (*models.Session, error) {
	oa, err := models.GetOrgAssets(ctx, rt, event.OrgID())
	if err != nil {
		return nil, errors.Wrapf(err, "error loading org")
	}

	// load the channel for this event
	channel := oa.ChannelByID(event.ChannelID())
	if channel == nil {
		slog.Info("ignoring event, couldn't find channel", "channel_id", event.ChannelID)
		return nil, nil
	}

	// load our contact
	modelContact, err := models.LoadContact(ctx, rt.ReadonlyDB, oa, event.ContactID())
	if err != nil {
		if err == sql.ErrNoRows { // if contact no longer exists, ignore event
			return nil, nil
		}
		return nil, errors.Wrapf(err, "error loading contact")
	}

	// if contact is blocked, ignore event
	if modelContact.Status() == models.ContactStatusBlocked {
		return nil, nil
	}

	if models.ContactSeenEvents[eventType] {
		// in the case of an incoming call this event isn't in the db and doesn't have created on
		lastSeenOn := event.CreatedOn()
		if lastSeenOn.IsZero() {
			lastSeenOn = dates.Now()
		}

		err = modelContact.UpdateLastSeenOn(ctx, rt.DB, lastSeenOn)
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
	case models.EventTypeNewConversation:
		trigger = models.FindMatchingNewConversationTrigger(oa, channel)
	case models.EventTypeReferral:
		trigger = models.FindMatchingReferralTrigger(oa, channel, event.ExtraString("referrer_id"))
	case models.EventTypeMissedCall:
		trigger = models.FindMatchingMissedCallTrigger(oa, channel)
	case models.EventTypeIncomingCall:
		trigger = models.FindMatchingIncomingCallTrigger(oa, channel, contact)
	case models.EventTypeOptIn:
		trigger = models.FindMatchingOptInTrigger(oa, channel)
	case models.EventTypeOptOut:
		trigger = models.FindMatchingOptOutTrigger(oa, channel)
	case models.EventTypeWelcomeMessage:
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
		slog.Info("ignoring channel event, no trigger found", "channel_id", event.ChannelID(), "event_type", eventType, "extra", event.Extra())
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

	// if this is an IVR flow and we don't have a call, trigger that asynchronously
	if flow.FlowType() == models.FlowTypeVoice && call == nil {
		err = TriggerIVRFlow(ctx, rt, oa.OrgID(), flow.ID(), []models.ContactID{modelContact.ID()}, nil)
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

	var flowOptIn *flows.OptIn
	if eventType == models.EventTypeOptIn || eventType == models.EventTypeOptOut {
		optIn := oa.OptInByID(event.OptInID())
		if optIn != nil {
			flowOptIn = oa.SessionAssets().OptIns().Get(optIn.UUID())
		}
	}

	// build our flow trigger
	tb := triggers.NewBuilder(oa.Env(), flow.Reference(), contact)
	var trig flows.Trigger

	if eventType == models.EventTypeIncomingCall {
		urn := modelContact.URNForID(event.URNID())
		trig = tb.Channel(channel.Reference(), triggers.ChannelEventTypeIncomingCall).WithCall(urn).Build()
	} else if eventType == models.EventTypeOptIn && flowOptIn != nil {
		trig = tb.OptIn(flowOptIn, triggers.OptInEventTypeStarted).Build()
	} else if eventType == models.EventTypeOptOut && flowOptIn != nil {
		trig = tb.OptIn(flowOptIn, triggers.OptInEventTypeStopped).Build()
	} else {
		trig = tb.Channel(channel.Reference(), triggers.ChannelEventType(eventType)).WithParams(params).Build()
	}

	// if we have a channel connection we set the connection on the session before our event hooks fire
	// so that IVR messages can be created with the right connection reference
	var hook models.SessionCommitHook
	if flow.FlowType() == models.FlowTypeVoice && call != nil {
		hook = func(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, oa *models.OrgAssets, sessions []*models.Session) error {
			for _, session := range sessions {
				session.SetCall(call)
			}
			return nil
		}
	}

	sessions, err := runner.StartFlowForContacts(ctx, rt, oa, flow, []*models.Contact{modelContact}, []flows.Trigger{trig}, hook, flow.FlowType().Interrupts())
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

	err = models.UpdateContactLastSeenOn(ctx, tx, event.ContactID, event.CreatedOn)
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
		return errors.Wrap(err, "error loading org")
	}

	// load the channel for this message
	channel := oa.ChannelByID(event.ChannelID)

	// fetch the attachments on the message (i.e. ask courier to fetch them)
	attachments := make([]utils.Attachment, 0, len(event.Attachments))
	logUUIDs := make([]models.ChannelLogUUID, 0, len(event.Attachments))

	// no channel, no attachments
	if channel != nil {
		for _, attURL := range event.Attachments {
			// if courier has already fetched this attachment, use it as is
			if utils.Attachment(attURL).ContentType() != "" {
				attachments = append(attachments, utils.Attachment(attURL))
			} else {
				attachment, logUUID, err := msgio.FetchAttachment(ctx, rt, channel, attURL, event.MsgID)
				if err != nil {
					return errors.Wrapf(err, "error fetching attachment '%s'", attURL)
				}

				attachments = append(attachments, attachment)
				logUUIDs = append(logUUIDs, logUUID)
			}
		}
	}

	// load our contact
	var db models.Queryer = rt.ReadonlyDB
	if event.NewContact {
		db = rt.DB // it might not be in the read replica yet
	}
	modelContact, err := models.LoadContact(ctx, db, oa, event.ContactID)
	if err != nil {
		return errors.Wrapf(err, "error loading contact")
	}

	// contact has been deleted, or is blocked, or channel no longer exists, ignore this message but mark it as handled
	if modelContact == nil || modelContact.Status() == models.ContactStatusBlocked || channel == nil {
		err := models.MarkMessageHandled(ctx, rt.DB, event.MsgID, models.MsgStatusHandled, models.VisibilityArchived, models.NilFlowID, models.NilTicketID, attachments, logUUIDs)
		if err != nil {
			return errors.Wrapf(err, "error updating message for deleted contact")
		}
		return nil
	}

	// if we have URNs make sure the message URN is our highest priority (this is usually a noop)
	if len(modelContact.URNs()) > 0 {
		err = modelContact.UpdatePreferredURN(ctx, rt.DB, oa, event.URNID, channel)
		if err != nil {
			return errors.Wrapf(err, "error changing primary URN")
		}
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

	// build our flow contact
	contact, err := modelContact.FlowContact(oa)
	if err != nil {
		return errors.Wrapf(err, "error creating flow contact")
	}

	// if this is a new contact, we need to calculate dynamic groups and campaigns
	if newContact {
		err = models.CalculateDynamicGroups(ctx, rt.DB, oa, []*flows.Contact{contact})
		if err != nil {
			return errors.Wrapf(err, "unable to initialize new contact")
		}
	}

	// look up any open tickes for this contact and forward this message to that
	ticket, err := models.LoadOpenTicketForContact(ctx, rt.DB, modelContact)
	if err != nil {
		return errors.Wrapf(err, "unable to look up open tickets for contact")
	}

	// find any matching triggers
	trigger, keyword := models.FindMatchingMsgTrigger(oa, channel, contact, event.Text)

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

	// flow will only see the attachments we were able to fetch
	availableAttachments := make([]utils.Attachment, 0, len(attachments))
	for _, att := range attachments {
		if att.ContentType() != utils.UnavailableType {
			availableAttachments = append(availableAttachments, att)
		}
	}

	msgIn := flows.NewMsgIn(event.MsgUUID, event.URN, channel.Reference(), event.Text, availableAttachments)
	msgIn.SetExternalID(string(event.MsgExternalID))
	msgIn.SetID(flows.MsgID(event.MsgID))

	// build our hook to mark a flow message as handled
	flowMsgHook := func(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, oa *models.OrgAssets, sessions []*models.Session) error {
		// set our incoming message event on our session
		if len(sessions) != 1 {
			return errors.Errorf("handle hook called with more than one session")
		}
		sessions[0].SetIncomingMsg(event.MsgID, event.MsgExternalID)

		return markMsgHandled(ctx, tx, contact, msgIn, flow, attachments, ticket, logUUIDs)
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
					return markMsgHandled(ctx, tx, contact, msgIn, flow, attachments, ticket, logUUIDs)
				}
				err = TriggerIVRFlow(ctx, rt, oa.OrgID(), flow.ID(), []models.ContactID{modelContact.ID()}, ivrMsgHook)
				if err != nil {
					return errors.Wrapf(err, "error while triggering ivr flow")
				}
				return nil
			}

			tb := triggers.NewBuilder(oa.Env(), flow.Reference(), contact).Msg(msgIn)
			if keyword != "" {
				tb = tb.WithMatch(&triggers.KeywordMatch{Type: trigger.KeywordMatchType(), Keyword: keyword})
			}

			// otherwise build the trigger and start the flow directly
			trigger := tb.Build()
			_, err = runner.StartFlowForContacts(ctx, rt, oa, flow, []*models.Contact{modelContact}, []flows.Trigger{trigger}, flowMsgHook, flow.FlowType().Interrupts())
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
	err = handleAsInbox(ctx, rt, oa, contact, msgIn, attachments, logUUIDs, ticket)
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
	modelContact, err := models.LoadContact(ctx, rt.ReadonlyDB, oa, modelTicket.ContactID())
	if err != nil {
		if err == sql.ErrNoRows { // if contact no longer exists, ignore event
			return nil
		}
		return errors.Wrapf(err, "error loading contact")
	}

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
		slog.Info("ignoring ticket event, no trigger found", "ticket_id", event.TicketID, "event_type", event.EventType())
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
		err = TriggerIVRFlow(ctx, rt, oa.OrgID(), flow.ID(), []models.ContactID{modelContact.ID()}, nil)
		if err != nil {
			return errors.Wrapf(err, "error while triggering ivr flow")
		}
		return nil
	}

	// build our flow ticket
	ticket := tickets[0].FlowTicket(oa)

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

	_, err = runner.StartFlowForContacts(ctx, rt, oa, flow, []*models.Contact{modelContact}, []flows.Trigger{flowTrigger}, nil, flow.FlowType().Interrupts())
	if err != nil {
		return errors.Wrapf(err, "error starting flow for contact")
	}
	return nil
}

func handleMsgDeletedEvent(ctx context.Context, rt *runtime.Runtime, event *MsgDeletedEvent) error {
	err := models.UpdateMessageDeletedBySender(ctx, rt.DB.DB, event.OrgID, event.MsgID)
	return errors.Wrap(err, "error deleting message")
}

// handles a message as an inbox message, i.e. no flow
func handleAsInbox(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, contact *flows.Contact, msg *flows.MsgIn, attachments []utils.Attachment, logUUIDs []models.ChannelLogUUID, ticket *models.Ticket) error {
	// usually last_seen_on is updated by handling the msg_received event in the engine sprint, but since this is an inbox
	// message we manually create that event and handle it
	msgEvent := events.NewMsgReceived(msg)
	contact.SetLastSeenOn(msgEvent.CreatedOn())
	contactEvents := map[*flows.Contact][]flows.Event{contact: {msgEvent}}

	err := models.HandleAndCommitEvents(ctx, rt, oa, models.NilUserID, contactEvents)
	if err != nil {
		return errors.Wrap(err, "error handling inbox message events")
	}

	return markMsgHandled(ctx, rt.DB, contact, msg, nil, attachments, ticket, logUUIDs)
}

// utility to mark as message as handled and update any open contact tickets
func markMsgHandled(ctx context.Context, db models.DBorTx, contact *flows.Contact, msg *flows.MsgIn, flow *models.Flow, attachments []utils.Attachment, ticket *models.Ticket, logUUIDs []models.ChannelLogUUID) error {
	flowID := models.NilFlowID
	if flow != nil {
		flowID = flow.ID()
	}
	ticketID := models.NilTicketID
	if ticket != nil {
		ticketID = ticket.ID()
	}

	err := models.MarkMessageHandled(ctx, db, models.MsgID(msg.ID()), models.MsgStatusHandled, models.VisibilityVisible, flowID, ticketID, attachments, logUUIDs)
	if err != nil {
		return errors.Wrapf(err, "error marking message as handled")
	}

	if ticket != nil {
		err = models.UpdateTicketLastActivity(ctx, db, []*models.Ticket{ticket})
		if err != nil {
			return errors.Wrapf(err, "error updating last activity for open ticket")
		}
	}

	return nil
}

type TimedEvent struct {
	ContactID models.ContactID `json:"contact_id"`
	OrgID     models.OrgID     `json:"org_id"`
	SessionID models.SessionID `json:"session_id"`
	Time      time.Time        `json:"time"`
}

type MsgEvent struct {
	ContactID     models.ContactID `json:"contact_id"`
	OrgID         models.OrgID     `json:"org_id"`
	ChannelID     models.ChannelID `json:"channel_id"`
	MsgID         models.MsgID     `json:"msg_id"`
	MsgUUID       flows.MsgUUID    `json:"msg_uuid"`
	MsgExternalID null.String      `json:"msg_external_id"`
	URN           urns.URN         `json:"urn"`
	URNID         models.URNID     `json:"urn_id"`
	Text          string           `json:"text"`
	Attachments   []string         `json:"attachments"`
	NewContact    bool             `json:"new_contact"`
}

type StopEvent struct {
	ContactID  models.ContactID `json:"contact_id"`
	OrgID      models.OrgID     `json:"org_id"`
	OccurredOn time.Time        `json:"occurred_on"`
	CreatedOn  time.Time        `json:"created_on"`
}

type MsgDeletedEvent struct {
	OrgID models.OrgID `json:"org_id"`
	MsgID models.MsgID `json:"message_id"`
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

type DBHook func(ctx context.Context, tx *sqlx.Tx) error

// TriggerIVRFlow will create a new flow start with the passed in flow and set of contacts. This will cause us to
// request calls to start, which once we get the callback will trigger our actual flow to start.
func TriggerIVRFlow(ctx context.Context, rt *runtime.Runtime, orgID models.OrgID, flowID models.FlowID, contactIDs []models.ContactID, hook DBHook) error {
	tx, _ := rt.DB.BeginTxx(ctx, nil)

	// create and insert our flow start
	start := models.NewFlowStart(orgID, models.StartTypeTrigger, flowID).WithContactIDs(contactIDs)
	err := models.InsertFlowStarts(ctx, tx, []*models.FlowStart{start})
	if err != nil {
		tx.Rollback()
		return errors.Wrapf(err, "error inserting ivr flow start")
	}

	// call our hook if we have one
	if hook != nil {
		err = hook(ctx, tx)
		if err != nil {
			tx.Rollback()
			return errors.Wrapf(err, "error while calling db hook")
		}
	}

	// commit our transaction
	err = tx.Commit()
	if err != nil {
		tx.Rollback()
		return errors.Wrapf(err, "error committing transaction for ivr flow starts")
	}

	// create our batch of all our contacts
	task := &ivr.StartIVRFlowBatchTask{FlowStartBatch: start.CreateBatch(contactIDs, models.FlowTypeVoice, true, len(contactIDs))}

	// queue this to our ivr starter, it will take care of creating the calls then calling back in
	rc := rt.RP.Get()
	defer rc.Close()
	err = tasks.Queue(rc, queue.BatchQueue, orgID, task, queue.HighPriority)
	if err != nil {
		return errors.Wrapf(err, "error queuing ivr flow start")
	}

	return nil
}
