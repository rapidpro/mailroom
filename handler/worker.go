package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/juju/errors"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/resumes"
	"github.com/nyaruka/goflow/flows/triggers"
	"github.com/nyaruka/mailroom"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/queue"
	"github.com/nyaruka/mailroom/runner"
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

	// find the topup for this message
	rc := rp.Get()
	topup, err := models.DecrementOrgCredits(ctx, db, rc, event.OrgID, 1)
	rc.Close()
	if err != nil {
		return errors.Annotatef(err, "error calculating topup for msg")
	}

	// load our contact
	contacts, err := models.LoadContacts(ctx, db, org, []flows.ContactID{event.ContactID})
	if err != nil {
		return errors.Annotatef(err, "error loading contact")
	}

	// contact has been deleted, ignore this message but mark it as handled
	if len(contacts) == 0 {
		err := models.UpdateMessage(ctx, db, event.MsgID, models.MsgStatusHandled, models.VisibilityArchived, models.TypeInbox, topup)
		if err != nil {
			return errors.Annotatef(err, "error updating message for deleted contact")
		}
		return nil
	}

	// build session assets
	sa, err := models.GetSessionAssets(org)
	if err != nil {
		return errors.Annotatef(err, "unable to load session assets")
	}

	modelContact := contacts[0]

	// load the channel for this message
	channel := org.ChannelByID(event.ChannelID)

	// make sure this URN is our highest priority (this is usually a noop)
	err = modelContact.UpdatePreferredURNAndChannel(ctx, db, event.URNID, channel)
	if err != nil {
		return errors.Annotatef(err, "error changing primary URN")
	}

	// build our flow contact
	contact, err := modelContact.FlowContact(org, sa)
	if err != nil {
		return errors.Annotatef(err, "error creating flow contact")
	}

	// if this channel is no longer active or this contact is blocked, ignore this message (mark it as handled)
	if channel == nil || modelContact.IsBlocked() {
		err := models.UpdateMessage(ctx, db, event.MsgID, models.MsgStatusHandled, models.VisibilityArchived, models.TypeInbox, topup)
		if err != nil {
			return errors.Annotatef(err, "error marking blocked or nil channel message as handled")
		}
		return nil
	}

	// stopped contact? they are unstopped if they send us an incoming message
	newContact := event.NewContact
	if modelContact.IsStopped() {
		err := modelContact.Unstop(ctx, db)
		if err != nil {
			return errors.Annotatef(err, "error unstopping contact")
		}

		newContact = true
	}

	// if this is a new contact, we need to calculate dynamic groups and campaigns
	if newContact {
		// TODO: perhaps this belongs on contact?
		orgGroups, _ := org.Groups()
		added, removed, errs := contact.ReevaluateDynamicGroups(org.Env(), flows.NewGroupAssets(orgGroups))
		if len(errs) > 0 {
			return errors.Annotatef(errs[0], "error calculating dynamic groups")
		}

		// start a transaction to commit all our changes at once
		tx, err := db.BeginTxx(ctx, nil)
		if err != nil {
			return errors.Annotatef(err, "unable to start transaction")
		}
		campaigns := make(map[models.CampaignID]*models.Campaign)

		groupAdds := make([]*models.GroupAdd, 0, 1)
		for _, a := range added {
			group := org.GroupByUUID(a.UUID())
			if group == nil {
				return errors.Annotatef(err, "added to unknown group: %s", a.UUID())
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
			return errors.Annotatef(err, "error adding contact to groups")
		}

		groupRemoves := make([]*models.GroupRemove, 0, 1)
		for _, r := range removed {
			group := org.GroupByUUID(r.UUID())
			if group == nil {
				return errors.Annotatef(err, "removed from an unknown group: %s", r.UUID())
			}
			groupRemoves = append(groupRemoves, &models.GroupRemove{
				ContactID: contact.ID(),
				GroupID:   group.ID(),
			})
		}
		err = models.RemoveContactsFromGroups(ctx, tx, groupRemoves)
		if err != nil {
			return errors.Annotatef(err, "error removing contact from group")
		}

		// for each campaign figure out if we need to be added to any events
		fireAdds := make([]*models.FireAdd, 0, 2)
		tz := org.Env().Timezone()
		now := time.Now()
		for _, c := range campaigns {
			for _, ce := range c.Events() {
				scheduled, err := ce.ScheduleForContact(tz, now, contact)
				if err != nil {
					return errors.Annotatef(err, "error calculating schedule for event: %d", ce.ID())
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
			return errors.Annotatef(err, "unable to add new event fires for contact")
		}

		// ok, commit everything
		err = tx.Commit()
		if err != nil {
			return errors.Annotatef(err, "unable to commit new contact updates")
		}
	}

	// find any matching triggers
	trigger := models.FindMatchingMsgTrigger(org, contact, event.Text)

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

	msgIn := flows.NewMsgIn(event.MsgUUID, event.MsgID, event.URN, channel.ChannelReference(), event.Text, event.Attachments)

	// TODO: how do we track the incoming message id / external_id for use in replies? seems like something we'd
	// ideally want on the session.

	// build our hook to mark our message as handled
	hook := func(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, sessions []*models.Session) error {
		// set our incoming message event on our session
		if len(sessions) != 1 {
			return errors.Errorf("handle hook called with more than one session")
		}
		sessions[0].SetIncomingMsg(event.MsgID, event.MsgExternalID)

		err = models.UpdateMessage(ctx, tx, event.MsgID, models.MsgStatusHandled, models.VisibilityVisible, models.TypeFlow, topup)
		if err != nil {
			return errors.Annotatef(err, "error marking message as handled")
		}
		return nil
	}

	// we found a trigger and their session is nil or doesn't ignore keywords
	if trigger != nil && (flow == nil || flow.IsArchived() || !flow.IgnoreTriggers()) {
		// start them in the triggered flow, interrupting their current flow/session
		match := &triggers.KeywordMatch{
			Type:    trigger.KeywordMatchType(),
			Keyword: trigger.Keyword(),
		}
		trigger := triggers.NewMsgTrigger(org.Env(), contact, flow.FlowReference(), nil, msgIn, match, time.Now())

		_, err = runner.StartFlowForContact(ctx, db, rp, org, sa, trigger, hook)
		if err != nil {
			return errors.Annotatef(err, "error starting flow for contact")
		}
		return nil
	}

	// if there is a session, resume it
	if flow != nil {
		resume := resumes.NewMsgResume(org.Env(), contact, msgIn)
		runner.ResumeFlow(ctx, db, rp, org, sa, session, resume, hook)
	}

	// this is a simple message, no session to resume and no trigger, stick it in our inbox
	err = models.UpdateMessage(ctx, db, event.MsgID, models.MsgStatusHandled, models.VisibilityVisible, models.TypeInbox, topup)
	if err != nil {
		return errors.Annotatef(err, "error marking message as handled")
	}
	return nil
}

type msgEvent struct {
	OrgID         models.OrgID       `json:"org_id"`
	ChannelID     models.ChannelID   `json:"channel_id"`
	ContactID     flows.ContactID    `json:"contact_id"`
	MsgID         flows.MsgID        `json:"msg_id"`
	MsgUUID       flows.MsgUUID      `json:"msg_uuid"`
	MsgExternalID string             `json:"external_id"`
	URN           urns.URN           `json:"urn"`
	URNID         models.URNID       `json:"urn_id"`
	Text          string             `json:"text"`
	Attachments   []flows.Attachment `json:"attachments"`
	NewContact    bool               `json:"new_contact"`
}

type handleEventTask struct {
	ContactID flows.ContactID `json:"contact_id"`
}
