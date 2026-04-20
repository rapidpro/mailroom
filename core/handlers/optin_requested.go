package handlers

import (
	"context"
	"log/slog"

	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/core/hooks"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/pkg/errors"
)

func init() {
	models.RegisterEventHandler(events.TypeOptInRequested, handleOptInRequested)
}

func handleOptInRequested(ctx context.Context, rt *runtime.Runtime, tx *sqlx.Tx, oa *models.OrgAssets, scene *models.Scene, e flows.Event) error {
	event := e.(*events.OptInRequestedEvent)

	slog.Debug("optin requested", "contact", scene.ContactUUID(), "session", scene.SessionID(), slog.Group("optin", "uuid", event.OptIn.UUID, "name", event.OptIn.Name))

	urn := event.URN
	var err error

	// messages in messaging flows must have urn id set on them, if not, go look it up
	if scene.Session().SessionType() == models.FlowTypeMessaging && event.URN != urns.NilURN {
		if models.GetURNInt(urn, "id") == 0 {
			urn, err = models.GetOrCreateURN(ctx, tx, oa, scene.ContactID(), event.URN)
			if err != nil {
				return errors.Wrapf(err, "unable to get or create URN: %s", event.URN)
			}
		}
	}

	// get our opt in
	optIn := oa.OptInByUUID(event.OptIn.UUID)
	if optIn == nil {
		return errors.Errorf("unable to load optin with uuid: %s", event.OptIn.UUID)
	}

	// get our channel
	channel := oa.ChannelByUUID(event.Channel.UUID)
	if channel == nil {
		return errors.Errorf("unable to load channel with uuid: %s", event.Channel.UUID)
	}

	// and the flow
	var flow *models.Flow
	run, _ := scene.Session().FindStep(e.StepUUID())
	flowAsset, _ := oa.FlowByUUID(run.FlowReference().UUID)
	if flowAsset != nil {
		flow = flowAsset.(*models.Flow)
	}

	msg := models.NewOutgoingOptInMsg(rt, scene.Session(), flow, optIn, channel, urn, event.CreatedOn())

	// register to have this message committed and sent
	scene.AppendToEventPreCommitHook(hooks.CommitMessagesHook, msg)
	scene.AppendToEventPostCommitHook(hooks.SendMessagesHook, msg)

	return nil
}
