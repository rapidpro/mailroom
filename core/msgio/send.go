package msgio

import (
	"context"
	"log/slog"
	"slices"

	"github.com/edganiukov/fcm"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
	"golang.org/x/exp/maps"
)

type Send struct {
	Msg *models.Msg
	URN *models.ContactURN
}

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
			slog.Error("error marking messages as initializing", "error", err)
		}
	}
}

func tryToQueue(ctx context.Context, rt *runtime.Runtime, db models.DBorTx, fc *fcm.Client, msgs []*models.Msg) []*models.Msg {
	// messages that have been successfully queued
	queued := make([]*models.Msg, 0, len(msgs))

	// fetch URNs and organize by id
	urnIDs := getMessageURNIDs(msgs)
	urnsByID := make(map[models.URNID]*models.ContactURN, len(urnIDs))
	for _, batch := range models.ChunkSlice(urnIDs, 1000) {
		urns, err := models.LoadContactURNs(ctx, db, batch)
		if err != nil {
			slog.Error("error getting contact URNs", "error", err)
			return nil
		}
		for _, u := range urns {
			urnsByID[u.ID] = u
		}
	}

	// organize what we have to send by org
	sendsByOrg := make(map[models.OrgID][]Send)
	for _, m := range msgs {
		orgID := m.OrgID()
		var urn *models.ContactURN
		if m.ContactURNID() != nil {
			urn = urnsByID[*m.ContactURNID()]
		}
		sendsByOrg[orgID] = append(sendsByOrg[orgID], Send{Msg: m, URN: urn})
	}

	for orgID, orgSends := range sendsByOrg {
		oa, err := models.GetOrgAssets(ctx, rt, orgID)
		if err != nil {
			slog.Error("error getting org assets", "error", err)
		} else {
			queued = append(queued, tryToQueueForOrg(ctx, rt, db, fc, oa, orgSends)...)
		}
	}

	return queued
}

func tryToQueueForOrg(ctx context.Context, rt *runtime.Runtime, db models.DBorTx, fc *fcm.Client, oa *models.OrgAssets, sends []Send) []*models.Msg {
	// sends by courier, organized by contact+channel
	courierSends := make(map[contactAndChannel][]Send, 100)

	// android channels that need to be notified to sync
	androidMsgs := make(map[*models.Channel][]*models.Msg, 100)

	// messages that have been successfully queued
	queued := make([]*models.Msg, 0, len(sends))

	for _, s := range sends {
		// ignore any message already marked as failed (maybe org is suspended)
		if s.Msg.Status() == models.MsgStatusFailed {
			queued = append(queued, s.Msg) // so that we don't try to requeue
			continue
		}

		channel := oa.ChannelByID(s.Msg.ChannelID())

		if channel != nil {
			if channel.Type() == models.ChannelTypeAndroid {
				androidMsgs[channel] = append(androidMsgs[channel], s.Msg)
			} else {
				cc := contactAndChannel{s.Msg.ContactID(), channel}
				courierSends[cc] = append(courierSends[cc], s)
			}
		}
	}

	// if there are courier messages to queue, do so
	if len(courierSends) > 0 {
		rc := rt.RP.Get()
		defer rc.Close()

		for cc, contactSends := range courierSends {
			err := QueueCourierMessages(rc, oa, cc.contactID, cc.channel, contactSends)

			// just log the error and continue to try - messages that weren't queued will be retried later
			if err != nil {
				slog.Error("error queuing messages", "error", err, "channel_uuid", cc.channel.UUID(), "contact_id", cc.contactID)
			} else {
				for _, s := range contactSends {
					queued = append(queued, s.Msg)
				}
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
				slog.Error("error syncing messages", "error", err, "channel_uuid", channel.UUID())
			}

			// even if syncing fails, we consider these messages queued because the device will try to sync by itself
			queued = append(queued, msgs...)
		}
	}

	return queued
}

// extracts the unique, non-nil contact URN ids for the given messages
func getMessageURNIDs(msgs []*models.Msg) []models.URNID {
	ids := make(map[models.URNID]bool, len(msgs))
	for _, m := range msgs {
		uid := m.ContactURNID()
		if uid != nil {
			ids[*uid] = true
		}
	}
	return maps.Keys(ids)
}

func assert(c bool, m string) {
	if !c {
		panic(m)
	}
}
