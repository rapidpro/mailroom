package msgio

import (
	"context"

	"github.com/edganiukov/fcm"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/sirupsen/logrus"
	"golang.org/x/exp/slices"
)

type contactAndChannel struct {
	contactID models.ContactID
	channel   *models.Channel
}

// QueueMessages tries to queue the given messages to courier or trigger Android channel syncs
func QueueMessages(ctx context.Context, rt *runtime.Runtime, db models.DBorTx, fc *fcm.Client, msgs []*models.Msg) {
	queued := tryToQueue(ctx, rt, db, fc, msgs)

	if len(queued) != len(msgs) {
		retry := make([]*models.Msg, 0, len(msgs)-len(queued))
		for _, m := range msgs {
			if !slices.Contains(queued, m) {
				retry = append(retry, m)
			}
		}

		// any messages that failed to queue should be moved back to initializing(I) (they are queued(Q) at creation to
		// save an update in the common case)
		err := models.MarkMessagesForRequeuing(ctx, db, retry)
		if err != nil {
			logrus.WithError(err).Error("error marking messages as initializing")
		}
	}
}

func tryToQueue(ctx context.Context, rt *runtime.Runtime, db models.DBorTx, fc *fcm.Client, msgs []*models.Msg) []*models.Msg {
	// messages that have been successfully queued
	queued := make([]*models.Msg, 0, len(msgs))

	// organize messages by org
	msgsByOrg := make(map[models.OrgID][]*models.Msg)
	for _, m := range msgs {
		msgsByOrg[m.OrgID()] = append(msgsByOrg[m.OrgID()], m)
	}

	for orgID, orgMsgs := range msgsByOrg {
		oa, err := models.GetOrgAssets(ctx, rt, orgID)
		if err != nil {
			logrus.WithError(err).Error("error getting org assets")
		} else {
			queued = append(queued, tryToQueueForOrg(ctx, rt, db, fc, oa, orgMsgs)...)
		}
	}

	return queued
}

func tryToQueueForOrg(ctx context.Context, rt *runtime.Runtime, db models.DBorTx, fc *fcm.Client, oa *models.OrgAssets, msgs []*models.Msg) []*models.Msg {
	// messages to be sent by courier, organized by contact+channel
	courierMsgs := make(map[contactAndChannel][]*models.Msg, 100)

	// android channels that need to be notified to sync
	androidMsgs := make(map[*models.Channel][]*models.Msg, 100)

	// messages that have been successfully queued
	queued := make([]*models.Msg, 0, len(msgs))

	for _, msg := range msgs {
		// ignore any message already marked as failed (maybe org is suspended)
		if msg.Status() == models.MsgStatusFailed {
			queued = append(queued, msg) // so that we don't try to requeue
			continue
		}

		channel := oa.ChannelByID(msg.ChannelID())

		if channel != nil {
			if channel.Type() == models.ChannelTypeAndroid {
				androidMsgs[channel] = append(androidMsgs[channel], msg)
			} else {
				cc := contactAndChannel{msg.ContactID(), channel}
				courierMsgs[cc] = append(courierMsgs[cc], msg)
			}
		}
	}

	// if there are courier messages to queue, do so
	if len(courierMsgs) > 0 {
		rc := rt.RP.Get()
		defer rc.Close()

		for cc, contactMsgs := range courierMsgs {
			err := QueueCourierMessages(rc, oa, cc.contactID, cc.channel, contactMsgs)

			// just log the error and continue to try - messages that weren't queued will be retried later
			if err != nil {
				logrus.WithField("channel_uuid", cc.channel.UUID()).WithField("contact_id", cc.contactID).WithError(err).Error("error queuing messages")
			} else {
				queued = append(queued, contactMsgs...)
			}
		}
	}

	// if we have any android messages, trigger syncs for the unique channels
	if len(androidMsgs) > 0 {
		if fc == nil {
			fc = CreateFCMClient(rt.Config)
		}

		for channel, msgs := range androidMsgs {
			err := SyncAndroidChannel(fc, channel)
			if err != nil {
				logrus.WithField("channel_uuid", channel.UUID()).WithError(err).Error("error syncing messages")
			}

			// even if syncing fails, we consider these messages queued because the device will try to sync by itself
			queued = append(queued, msgs...)
		}
	}

	return queued
}

func assert(c bool, m string) {
	if !c {
		panic(m)
	}
}
