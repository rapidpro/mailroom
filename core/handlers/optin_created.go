package handlers

import (
	"context"

	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/core/hooks"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func init() {
	models.RegisterEventHandler(events.TypeOptInCreated, handleOptInCreated)
}

func handleOptInCreated(ctx context.Context, rt *runtime.Runtime, tx *sqlx.Tx, oa *models.OrgAssets, scene *models.Scene, e flows.Event) error {
	event := e.(*events.OptInCreatedEvent)

	logrus.WithFields(logrus.Fields{"contact_uuid": scene.ContactUUID(), "session_id": scene.SessionID(), "optin": event.OptIn.Name, "urn": event.URN}).Debug("optin created event")

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

	msg, err := models.NewOutgoingOptInMsg(rt, oa.Org(), channel, scene.Session(), optIn, urn, event.CreatedOn())
	if err != nil {
		return errors.Wrap(err, "error creating outgoing message")
	}

	// register to have this message committed
	scene.AppendToEventPreCommitHook(hooks.CommitMessagesHook, msg)

	return nil
}
