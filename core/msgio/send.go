package msgio

import (
	"context"

	"github.com/edganiukov/fcm"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"

	"github.com/apex/log"
)

// SendMessages tries to send the given messages via Courier or Android syncing
func SendMessages(ctx context.Context, rt *runtime.Runtime, tx models.Queryer, fc *fcm.Client, msgs []*models.Msg) {
	// messages to be sent by courier, organized by contact
	courierMsgs := make(map[models.ContactID][]*models.Msg, 100)

	// android channels that need to be notified to sync
	androidChannels := make([]*models.Channel, 0, 5)
	androidChannelsSeen := make(map[*models.Channel]bool)

	// messages that need to be marked as pending
	pending := make([]*models.Msg, 0, 1)

	// walk through our messages, separate by whether they have a channel and if it's Android
	for _, msg := range msgs {
		// ignore any message already marked as failed (maybe org is suspended)
		if msg.Status() == models.MsgStatusFailed {
			continue
		}

		channel := msg.Channel()
		if channel != nil {
			if channel.Type() == models.ChannelTypeAndroid {
				if !androidChannelsSeen[channel] {
					androidChannels = append(androidChannels, channel)
				}
				androidChannelsSeen[channel] = true
			} else {
				courierMsgs[msg.ContactID()] = append(courierMsgs[msg.ContactID()], msg)
			}
		} else {
			pending = append(pending, msg)
		}
	}

	// if there are courier messages to send, do so
	if len(courierMsgs) > 0 {
		rc := rt.RP.Get()
		defer rc.Close()

		for contactID, contactMsgs := range courierMsgs {
			err := QueueCourierMessages(rc, contactID, contactMsgs)

			// not being able to queue a message isn't the end of the world, log but don't return an error
			if err != nil {
				log.WithField("messages", contactMsgs).WithField("contact", contactID).WithError(err).Error("error queuing messages")

				// in the case of errors we do want to change the messages back to pending however so they
				// get queued later. (for the common case messages are only inserted and queued, without a status update)
				pending = append(pending, contactMsgs...)
			}
		}
	}

	// if we have any android messages, trigger syncs for the unique channels
	if len(androidChannels) > 0 {
		if fc == nil {
			fc = CreateFCMClient(rt.Config)
		}
		SyncAndroidChannels(fc, androidChannels)
	}

	// any messages that didn't get sent should be moved back to pending (they are queued at creation to save an
	// update in the common case)
	if len(pending) > 0 {
		err := models.MarkMessagesPending(ctx, tx, pending)
		if err != nil {
			log.WithError(err).Error("error marking message as pending")
		}
	}
}

func assert(c bool, m string) {
	if !c {
		panic(m)
	}
}
