package hooks

import (
	"context"
	"time"

	"github.com/nyaruka/mailroom/config"
	"github.com/nyaruka/mailroom/core/courier"
	"github.com/nyaruka/mailroom/core/models"

	"github.com/apex/log"
	"github.com/edganiukov/fcm"
	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"
)

// SendMessagesHook is our hook for sending scene messages
var SendMessagesHook models.EventCommitHook = &sendMessagesHook{}

type sendMessagesHook struct{}

// Apply sends all non-android messages to courier
func (h *sendMessagesHook) Apply(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, oa *models.OrgAssets, scenes map[*models.Scene][]interface{}) error {
	rc := rp.Get()
	defer rc.Close()

	// messages that need to be marked as pending
	pending := make([]*models.Msg, 0, 1)

	// android channels that need to be notified to sync
	androidChannels := make(map[*models.Channel]bool)

	// for each scene gather all our messages
	for s, args := range scenes {
		// walk through our messages, separate by whether they're android or not
		courierMsgs := make([]*models.Msg, 0, len(args))

		for _, m := range args {
			msg := m.(*models.Msg)
			channel := msg.Channel()
			if channel != nil {
				if channel.Type() == models.ChannelTypeAndroid {
					androidChannels[channel] = true
				} else {
					courierMsgs = append(courierMsgs, msg)
				}
			} else {
				pending = append(pending, msg)
			}
		}

		// if there are courier messages to send, do so
		if len(courierMsgs) > 0 {
			// if our scene has a timeout, set it on our last message
			if s.Session().Timeout() != nil && s.Session().WaitStartedOn() != nil {
				courierMsgs[len(courierMsgs)-1].SetTimeout(*s.Session().WaitStartedOn(), *s.Session().Timeout())
			}

			log := log.WithField("messages", courierMsgs).WithField("scene", s.SessionID)

			err := courier.QueueMessages(rc, courierMsgs)

			// not being able to queue a message isn't the end of the world, log but don't return an error
			if err != nil {
				log.WithError(err).Error("error queuing message")

				// in the case of errors we do want to change the messages back to pending however so they
				// get queued later. (for the common case messages are only inserted and queued, without a status update)
				for _, msg := range courierMsgs {
					pending = append(pending, msg)
				}
			}
		}
	}

	// if we have any android messages, trigger syncs for the unique channels
	for channel := range androidChannels {
		// no FCM key for this rapidpro install? break out but log
		if config.Mailroom.FCMKey == "" {
			logrus.Error("cannot trigger sync for android channel, FCM Key unset")
			break
		}

		// no fcm id for this channel, noop, we can't trigger a sync
		fcmID := channel.ConfigValue(models.ChannelConfigFCMID, "")
		if fcmID == "" {
			continue
		}

		client, err := fcm.NewClient(config.Mailroom.FCMKey)
		if err != nil {
			logrus.WithError(err).Error("error initializing fcm client")
			continue
		}

		sync := &fcm.Message{
			Token:       fcmID,
			Priority:    "high",
			CollapseKey: "sync",
			Data: map[string]interface{}{
				"msg": "sync",
			},
		}

		start := time.Now()
		_, err = client.Send(sync)

		if err != nil {
			// log failures but continue, relayer will sync on its own
			logrus.WithError(err).WithField("channel_uuid", channel.UUID()).Error("error syncing channel")
		} else {
			logrus.WithField("elapsed", time.Since(start)).WithField("channel_uuid", channel.UUID()).Debug("android sync complete")
		}
	}

	// any messages that didn't get sent should be moved back to pending (they are queued at creation to save an
	// update in the common case)
	if len(pending) > 0 {
		err := models.MarkMessagesPending(ctx, tx, pending)
		if err != nil {
			log.WithError(err).Error("error marking message as pending")
		}
	}

	return nil
}
