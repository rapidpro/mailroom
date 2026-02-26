package handlers

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/runner"
	"github.com/nyaruka/mailroom/core/runner/hooks"
	"github.com/nyaruka/mailroom/core/search"
	"github.com/nyaruka/mailroom/runtime"
)

func init() {
	runner.RegisterEventHandler(events.TypeMsgCreated, handleMsgCreated)
}

// handleMsgCreated creates the db msg for the passed in event
func handleMsgCreated(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, scene *runner.Scene, e flows.Event, userID models.UserID) error {
	event := e.(*events.MsgCreated)

	slog.Debug("msg created", "contact", scene.ContactUUID(), "session", scene.SessionUUID(), "text", event.Msg.Text(), "urn", event.Msg.URN())

	// get our channel
	var channel *models.Channel
	if event.Msg.Channel() != nil {
		channel = oa.ChannelByUUID(event.Msg.Channel().UUID)
		if channel == nil {
			return fmt.Errorf("unable to load channel with uuid: %s", event.Msg.Channel().UUID)
		}
	}

	var msg *models.MsgOut
	var err error

	if event.BroadcastUUID != "" {
		msg, err = models.NewOutgoingBroadcastMsg(rt, oa.Org(), channel, scene.DBContact, event, scene.Broadcast)
	} else if userID != models.NilUserID {
		msg, err = models.NewOutgoingChatMsg(rt, oa.Org(), channel, scene.DBContact, event, userID)
	} else {
		flow := e.Step().Run().Flow().Asset().(*models.Flow)

		msg, err = models.NewOutgoingFlowMsg(rt, oa.Org(), channel, scene.DBContact, flow, event, scene.IncomingMsg)
	}
	if err != nil {
		return fmt.Errorf("error creating outgoing message to %s: %w", event.Msg.URN(), err)
	}

	// commit this message in the transaction
	scene.AttachPreCommitHook(hooks.InsertMessages, hooks.MsgAndURN{Msg: msg.Msg, URN: event.Msg.URN()})

	// and queue it to be sent after the transaction is complete
	if event.Msg.UnsendableReason() == "" {
		scene.AttachPostCommitHook(hooks.SendMessages, msg)
	}

	scene.OutgoingMsgs = append(scene.OutgoingMsgs, msg)

	// index message to OpenSearch if it's not from a broadcast, flow, or IVR
	if event.BroadcastUUID == "" && userID != models.NilUserID && scene.Call == nil && len(event.Msg.Text()) >= search.MessageTextMinLength {
		scene.AttachPostCommitHook(hooks.IndexMessages, &search.MessageDoc{
			CreatedOn:   event.CreatedOn(),
			OrgID:       oa.OrgID(),
			UUID:        event.UUID(),
			ContactUUID: scene.ContactUUID(),
			Text:        event.Msg.Text(),
			InTicket:    event.TicketUUID != "",
		})
	}

	return nil
}
