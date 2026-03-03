package ctasks

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/goflow/flows/modifiers"
	"github.com/nyaruka/goflow/flows/triggers"
	"github.com/nyaruka/mailroom/core/ivr"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/runner"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/null/v3"
)

const TypeEventReceived = "event_received"

func init() {
	RegisterType(TypeEventReceived, func() Task { return &EventReceived{} })
}

type EventReceived struct {
	EventUUID  models.ChannelEventUUID `json:"event_uuid"`
	EventType  models.ChannelEventType `json:"event_type"`
	ChannelID  models.ChannelID        `json:"channel_id"`
	URNID      models.URNID            `json:"urn_id"`
	OptInID    models.OptInID          `json:"optin_id"`
	Extra      null.Map[any]           `json:"extra"`
	NewContact bool                    `json:"new_contact"`
}

func (t *EventReceived) Type() string {
	return TypeEventReceived
}

func (t *EventReceived) Perform(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, mc *models.Contact) error {
	_, err := t.handle(ctx, rt, oa, mc, nil)
	if err != nil {
		return fmt.Errorf("error handling channel event %s: %w", t.EventUUID, err)
	}

	return models.MarkChannelEventHandled(ctx, rt.DB, t.EventUUID)
}

// Handle let's us reuse this task's code for handling incoming calls.. which we need to perform inline in the IVR web
// handler rather than as a queued task.
func (t *EventReceived) Handle(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, mc *models.Contact, call *models.Call) (*runner.Scene, error) {
	return t.handle(ctx, rt, oa, mc, call)
}

func (t *EventReceived) handle(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, mc *models.Contact, call *models.Call) (*runner.Scene, error) {
	channel := oa.ChannelByID(t.ChannelID)

	// if contact is blocked or channel no longer exists, nothing to do
	if mc.Status() == models.ContactStatusBlocked || channel == nil {
		return nil, nil
	}

	urn := mc.GetURN(t.URNID)

	if t.EventType == models.EventTypeDeleteContact {
		slog.Info("delete contact event ignored", "contact", mc.UUID(), "event", t.EventUUID)

		return nil, nil
	}

	// build our flow contact
	contact, err := mc.EngineContact(oa)
	if err != nil {
		return nil, fmt.Errorf("error creating flow contact: %w", err)
	}

	var flowOptIn *flows.OptIn
	if t.EventType == models.EventTypeOptIn || t.EventType == models.EventTypeOptOut {
		optIn := oa.OptInByID(t.OptInID)
		if optIn != nil {
			flowOptIn = oa.SessionAssets().OptIns().Get(optIn.UUID())
		}
	}

	var flowCall *flows.Call
	if call != nil {
		flowCall = flows.NewCall(call.UUID(), oa.SessionAssets().Channels().Get(channel.UUID()), urn.Identity)
	}

	scene := runner.NewScene(mc, contact)
	scene.DBCall = call
	scene.Call = flowCall

	if t.NewContact {
		if err := scene.ReevaluateGroups(ctx, rt, oa); err != nil {
			return nil, fmt.Errorf("error calculating groups for new contact: %w", err)
		}
	} else if contact.Status() == flows.ContactStatusStopped && (t.EventType == models.EventTypeNewConversation || t.EventType == models.EventTypeReferral) {
		if err := scene.ApplyModifier(ctx, rt, oa, modifiers.NewStatus(flows.ContactStatusActive), models.NilUserID, ""); err != nil {
			return nil, fmt.Errorf("error applying modifier to unstop contact: %w", err)
		}
	}

	// make sure this URN is our highest priority (this is usually a noop)
	if t.URNID != models.NilURNID && urn != nil {
		if ch := oa.SessionAssets().Channels().Get(channel.UUID()); ch != nil {
			if err := scene.ApplyModifier(ctx, rt, oa, modifiers.NewAffinity(urn.Identity, ch), models.NilUserID, ""); err != nil {
				return nil, fmt.Errorf("error applying affinity modifier: %w", err)
			}
		}
	}

	// convert to real event
	event := t.toEvent(channel, flowCall, flowOptIn)
	if event != nil {
		if err := scene.AddEvent(ctx, rt, oa, event, models.NilUserID, ""); err != nil {
			return nil, fmt.Errorf("error adding channel event to scene: %w", err)
		}

		trig, flowType, err := findEventTrigger(oa, event, channel, contact, flowOptIn)
		if err != nil {
			return nil, err
		}

		if trig != nil {
			if flowType == models.FlowTypeVoice && call == nil {
				// request outgoing call and wait for callback
				if _, err := ivr.RequestCall(ctx, rt, oa, mc, trig); err != nil {
					return nil, fmt.Errorf("error requesting call: %w", err)
				}
			} else {
				if err := scene.StartSession(ctx, rt, oa, trig, flowType.Interrupts()); err != nil {
					return nil, fmt.Errorf("error starting session: %w", err)
				}
			}
		}
	}

	if t.EventType == models.EventTypeStopContact {
		if err := scene.ApplyModifier(ctx, rt, oa, modifiers.NewStatus(flows.ContactStatusStopped), models.NilUserID, ""); err != nil {
			return nil, fmt.Errorf("error applying stop modifier: %w", err)
		}
	}

	// update last_seen_on last so that during flow execution it's the previous value which is more useful than now
	if err := scene.ApplyModifier(ctx, rt, oa, modifiers.NewSeen(dates.Now()), models.NilUserID, ""); err != nil {
		return nil, fmt.Errorf("error applying last seen modifier: %w", err)
	}

	if err := scene.Commit(ctx, rt, oa); err != nil {
		return nil, fmt.Errorf("error committing scene: %w", err)
	}

	return scene, nil
}

// convert to a real engine event
func (t *EventReceived) toEvent(ch *models.Channel, call *flows.Call, optIn *flows.OptIn) flows.Event {
	switch t.EventType {
	case models.EventTypeMissedCall:
		return events.NewCallMissed(ch.Reference())
	case models.EventTypeIncomingCall:
		return events.NewCallReceived(call)
	case models.EventTypeNewConversation:
		return events.NewChatStarted(ch.Reference(), nil)
	case models.EventTypeReferral:
		var params map[string]string
		if t.Extra != nil {
			params = make(map[string]string, len(t.Extra))
			for k, v := range t.Extra {
				if vs, ok := v.(string); ok {
					params[k] = vs
				}
			}
		}
		return events.NewChatStarted(ch.Reference(), params)
	case models.EventTypeOptIn:
		if optIn != nil {
			return events.NewOptInStarted(optIn.Reference(), ch.Reference())
		}
	case models.EventTypeOptOut:
		if optIn != nil {
			return events.NewOptInStopped(optIn.Reference(), ch.Reference())
		}
	}
	return nil
}

func findEventTrigger(oa *models.OrgAssets, evt flows.Event, ch *models.Channel, c *flows.Contact, optIn *flows.OptIn) (flows.Trigger, models.FlowType, error) {
	var mtrig *models.Trigger

	switch typed := evt.(type) {
	case *events.CallMissed:
		mtrig = models.FindMatchingMissedCallTrigger(oa, ch)
	case *events.CallReceived:
		mtrig = models.FindMatchingIncomingCallTrigger(oa, ch, c)
	case *events.ChatStarted:
		if len(typed.Params) > 0 {
			mtrig = models.FindMatchingReferralTrigger(oa, ch, typed.Params["referrer_id"])
		} else {
			mtrig = models.FindMatchingNewConversationTrigger(oa, ch)
		}
	case *events.OptInStarted:
		mtrig = models.FindMatchingOptInTrigger(oa, ch)
	case *events.OptInStopped:
		mtrig = models.FindMatchingOptOutTrigger(oa, ch)
	default:
		panic(fmt.Sprintf("unknown event type: %T", evt))
	}

	// check flow still exists
	var flow *models.Flow
	var err error
	if mtrig != nil {
		flow, err = oa.FlowByID(mtrig.FlowID())
		if err != nil && err != models.ErrNotFound {
			return nil, "", fmt.Errorf("error loading flow for trigger: %w", err)
		}
	}

	// no trigger or flow gone, nothing to do
	if flow == nil {
		return nil, "", nil
	}

	// build engine trigger
	var trig flows.Trigger
	tb := triggers.NewBuilder(flow.Reference())

	switch typed := evt.(type) {
	case *events.CallMissed:
		trig = tb.CallMissed(typed).Build()
	case *events.CallReceived:
		trig = tb.CallReceived(typed).Build()
	case *events.ChatStarted:
		trig = tb.ChatStarted(typed).Build()
	case *events.OptInStarted:
		trig = tb.OptInStarted(typed, optIn).Build()
	case *events.OptInStopped:
		trig = tb.OptInStopped(typed, optIn).Build()
	default:
		panic(fmt.Sprintf("unknown event type: %T", evt))
	}

	return trig, flow.FlowType(), nil
}
