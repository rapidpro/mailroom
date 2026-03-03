package ctasks

import (
	"context"
	"fmt"

	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/goflow/flows/modifiers"
	"github.com/nyaruka/goflow/flows/resumes"
	"github.com/nyaruka/goflow/flows/triggers"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/core/ivr"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/msgio"
	"github.com/nyaruka/mailroom/core/runner"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/utils/clogs"
)

const TypeMsgReceived = "msg_received"

func init() {
	RegisterType(TypeMsgReceived, func() Task { return &MsgReceived{} })
}

type MsgReceived struct {
	MsgUUID       flows.EventUUID  `json:"msg_uuid"`
	MsgExternalID string           `json:"msg_external_id"`
	ChannelID     models.ChannelID `json:"channel_id"`
	URN           urns.URN         `json:"urn"`
	URNID         models.URNID     `json:"urn_id"`
	Text          string           `json:"text"`
	Attachments   []string         `json:"attachments,omitempty"`
	NewContact    bool             `json:"new_contact"`
}

func (t *MsgReceived) Type() string {
	return TypeMsgReceived
}

func (t *MsgReceived) Perform(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, mc *models.Contact) error {
	return t.perform(ctx, rt, oa, mc)
}

func (t *MsgReceived) perform(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, mc *models.Contact) error {
	channel := oa.ChannelByID(t.ChannelID)

	// fetch the attachments on the message (i.e. ask courier to fetch them)
	attachments := make([]utils.Attachment, 0, len(t.Attachments))
	logUUIDs := make([]clogs.UUID, 0, len(t.Attachments))

	// no channel, no attachments
	if channel != nil {
		for _, attURL := range t.Attachments {
			// if courier has already fetched this attachment, use it as is
			if utils.Attachment(attURL).ContentType() != "" {
				attachments = append(attachments, utils.Attachment(attURL))
			} else {
				attachment, logUUID, err := msgio.FetchAttachment(ctx, rt, channel, attURL, t.MsgUUID)
				if err != nil {
					return fmt.Errorf("error fetching attachment '%s': %w", attURL, err)
				}

				attachments = append(attachments, attachment)
				logUUIDs = append(logUUIDs, logUUID)
			}
		}
	}

	// flow will only see the attachments we were able to fetch
	availableAttachments := make([]utils.Attachment, 0, len(attachments))
	for _, att := range attachments {
		if att.ContentType() != utils.UnavailableType {
			availableAttachments = append(availableAttachments, att)
		}
	}

	// associate this message with the last open ticket for this contact if there is one
	var ticketUUID flows.TicketUUID
	if tks := mc.Tickets(); len(tks) > 0 {
		ticketUUID = tks[len(tks)-1].UUID
	}

	msgIn := flows.NewMsgIn(t.URN, channel.Reference(), t.Text, availableAttachments, string(t.MsgExternalID))
	msgEvent := events.NewMsgReceived(msgIn, ticketUUID)
	msgEvent.UUID_ = t.MsgUUID

	// build our flow contact
	contact, err := mc.EngineContact(oa)
	if err != nil {
		return fmt.Errorf("error creating flow contact: %w", err)
	}

	scene := runner.NewScene(mc, contact)
	scene.IncomingMsg = &models.MsgInRef{
		UUID:        t.MsgUUID,
		ExtID:       t.MsgExternalID,
		Attachments: attachments,
		LogUUIDs:    logUUIDs,
	}

	if t.NewContact {
		if err := scene.ReevaluateGroups(ctx, rt, oa); err != nil {
			return fmt.Errorf("error calculating groups for new contact: %w", err)
		}
	} else if contact.Status() == flows.ContactStatusStopped {
		// if we get a message from a stopped contact, unstop them
		if err := scene.ApplyModifier(ctx, rt, oa, modifiers.NewStatus(flows.ContactStatusActive), models.NilUserID, ""); err != nil {
			return fmt.Errorf("error applying modifier to unstop contact: %w", err)
		}
	}

	// if we have URNs make sure the message URN is our highest priority (this is usually a noop)
	if len(mc.URNs()) > 0 && channel != nil {
		if ch := oa.SessionAssets().Channels().Get(channel.UUID()); ch != nil {
			if err := scene.ApplyModifier(ctx, rt, oa, modifiers.NewAffinity(t.URN, ch), models.NilUserID, ""); err != nil {
				return fmt.Errorf("error applying affinity modifier: %w", err)
			}
		}
	}

	if err := scene.AddEvent(ctx, rt, oa, msgEvent, models.NilUserID, ""); err != nil {
		return fmt.Errorf("error adding message event to scene: %w", err)
	}

	if err := t.handleMsgEvent(ctx, rt, oa, channel, scene, msgEvent); err != nil {
		return fmt.Errorf("error handing message event in scene: %w", err)
	}

	// update last_seen_on last so that during flow execution it's the previous value which is more useful than now
	if err := scene.ApplyModifier(ctx, rt, oa, modifiers.NewSeen(dates.Now()), models.NilUserID, ""); err != nil {
		return fmt.Errorf("error applying last seen modifier: %w", err)
	}

	if err := scene.Commit(ctx, rt, oa); err != nil {
		return fmt.Errorf("error committing scene: %w", err)
	}

	return nil
}

func (t *MsgReceived) handleMsgEvent(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, channel *models.Channel, scene *runner.Scene, msgEvent *events.MsgReceived) error {
	// if contact is blocked, or channel no longer exists or is disabled, no sessions
	if scene.Contact.Status() == flows.ContactStatusBlocked || channel == nil {
		return nil
	}

	// look for a waiting session for this contact
	session, err := models.GetContactWaitingSession(ctx, rt, oa, scene.DBContact)
	if err != nil {
		return fmt.Errorf("error loading waiting session for contact %s: %w", scene.ContactUUID(), err)
	}

	var flow *models.Flow

	if session != nil {
		// if we have a waiting voice session, we want to leave it as is
		if session.SessionType == models.FlowTypeVoice {
			return nil
		}

		// get the flow to be resumed and if it's gone, end the session
		flowAsset, err := oa.FlowByUUID(session.CurrentFlowUUID)
		if err == models.ErrNotFound {
			if err := scene.InterruptWaiting(ctx, rt, oa, flows.SessionStatusFailed); err != nil {
				return fmt.Errorf("error ending session %s: %w", session.UUID, err)
			}
			session = nil
		} else if err != nil {
			return fmt.Errorf("error loading flow for session: %w", err)
		} else {
			flow = flowAsset.(*models.Flow)
		}
	}

	// find any matching triggers
	trigger, keyword := models.FindMatchingMsgTrigger(oa, channel, scene.Contact, t.Text)

	// we found a trigger and their session is nil or doesn't ignore keywords
	if (trigger != nil && trigger.TriggerType() != models.CatchallTriggerType && (flow == nil || !flow.IgnoreTriggers())) ||
		(trigger != nil && trigger.TriggerType() == models.CatchallTriggerType && (flow == nil)) {

		// load flow to check it's still accessible
		flow, err = oa.FlowByID(trigger.FlowID())
		if err != nil && err != models.ErrNotFound {
			return fmt.Errorf("error loading flow for trigger: %w", err)
		}

		if flow != nil {
			// create trigger from this message
			tb := triggers.NewBuilder(flow.Reference()).MsgReceived(msgEvent)
			if keyword != "" {
				tb = tb.WithMatch(&triggers.KeywordMatch{Type: trigger.KeywordMatchType(), Keyword: keyword})
			}
			flowTrigger := tb.Build()

			// if this is a voice flow, we request a call and wait for callback
			if flow.FlowType() == models.FlowTypeVoice {
				if _, err := ivr.RequestCall(ctx, rt, oa, scene.DBContact, flowTrigger); err != nil {
					return fmt.Errorf("error starting voice flow for contact: %w", err)
				}
			} else {
				if err := scene.StartSession(ctx, rt, oa, flowTrigger, flow.FlowType().Interrupts()); err != nil {
					return fmt.Errorf("error starting session for contact %s: %w", scene.ContactUUID(), err)
				}
			}

			return nil
		}
	}

	// if there is a session, resume it
	if session != nil && flow != nil {
		if err := scene.ResumeSession(ctx, rt, oa, session, resumes.NewMsg(msgEvent)); err != nil {
			return fmt.Errorf("error resuming flow for contact: %w", err)
		}
	}

	return nil
}
