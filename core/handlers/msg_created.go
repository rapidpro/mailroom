package handlers

import (
	"context"

	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/core/hooks"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func init() {
	models.RegisterEventPreWriteHandler(events.TypeMsgCreated, handlePreMsgCreated)
	models.RegisterEventHandler(events.TypeMsgCreated, handleMsgCreated)
}

// handlePreMsgCreated clears our timeout on our session so that courier can send it when the message is sent, that will be set by courier when sent
func handlePreMsgCreated(ctx context.Context, rt *runtime.Runtime, tx *sqlx.Tx, oa *models.OrgAssets, scene *models.Scene, e flows.Event) error {
	event := e.(*events.MsgCreatedEvent)

	// we only clear timeouts on messaging flows
	if scene.Session().SessionType() != models.FlowTypeMessaging {
		return nil
	}

	// get our channel
	var channel *models.Channel

	if event.Msg.Channel() != nil {
		channel = oa.ChannelByUUID(event.Msg.Channel().UUID)
		if channel == nil {
			return errors.Errorf("unable to load channel with uuid: %s", event.Msg.Channel().UUID)
		}
	}

	// no channel? this is a no-op
	if channel == nil {
		return nil
	}

	// android channels get normal timeouts
	if channel.Type() == models.ChannelTypeAndroid {
		return nil
	}

	// everybody else gets their timeout cleared, will be set by courier
	scene.Session().ClearTimeoutOn()

	return nil
}

// handleMsgCreated creates the db msg for the passed in event
func handleMsgCreated(ctx context.Context, rt *runtime.Runtime, tx *sqlx.Tx, oa *models.OrgAssets, scene *models.Scene, e flows.Event) error {
	event := e.(*events.MsgCreatedEvent)

	// must be in a session
	if scene.Session() == nil {
		return errors.Errorf("cannot handle msg created event without session")
	}

	logrus.WithFields(logrus.Fields{
		"contact_uuid": scene.ContactUUID(),
		"session_id":   scene.SessionID(),
		"text":         event.Msg.Text(),
		"urn":          event.Msg.URN(),
	}).Debug("msg created event")

	// messages in messaging flows must have urn id set on them, if not, go look it up
	if scene.Session().SessionType() == models.FlowTypeMessaging && event.Msg.URN() != urns.NilURN {
		urn := event.Msg.URN()
		if models.GetURNInt(urn, "id") == 0 {
			urn, err := models.GetOrCreateURN(ctx, tx, oa, scene.ContactID(), event.Msg.URN())
			if err != nil {
				return errors.Wrapf(err, "unable to get or create URN: %s", event.Msg.URN())
			}
			// update our Msg with our full URN
			event.Msg.SetURN(urn)
		}
	}

	// get our channel
	var channel *models.Channel
	if event.Msg.Channel() != nil {
		channel = oa.ChannelByUUID(event.Msg.Channel().UUID)
		if channel == nil {
			return errors.Errorf("unable to load channel with uuid: %s", event.Msg.Channel().UUID)
		}
	}

	run, _ := scene.Session().FindStep(e.StepUUID())
	flow, _ := oa.FlowByUUID(run.FlowReference().UUID)

	msg, err := models.NewOutgoingFlowMsg(rt, oa.Org(), channel, scene.Session(), flow.(*models.Flow), event.Msg, event.CreatedOn())
	if err != nil {
		return errors.Wrapf(err, "error creating outgoing message to %s", event.Msg.URN())
	}

	// register to have this message committed
	scene.AppendToEventPreCommitHook(hooks.CommitMessagesHook, msg)

	// don't send messages for surveyor flows
	if scene.Session().SessionType() != models.FlowTypeSurveyor {
		scene.AppendToEventPostCommitHook(hooks.SendMessagesHook, msg)
	}

	return nil
}
