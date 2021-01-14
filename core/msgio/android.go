package msgio

import (
	"time"

	"github.com/nyaruka/mailroom/core/models"

	"github.com/edganiukov/fcm"
	"github.com/sirupsen/logrus"
)

// SyncAndroidChannels tries to trigger syncs of the given Android channels via FCM
func SyncAndroidChannels(fc *fcm.Client, channels []*models.Channel) {
	if fc == nil {
		logrus.Warn("skipping Android sync as instance has not configured FCM")
		return
	}

	for _, channel := range channels {
		// no FCM ID for this channel, noop, we can't trigger a sync
		fcmID := channel.ConfigValue(models.ChannelConfigFCMID, "")
		if fcmID == "" {
			continue
		}

		sync := &fcm.Message{
			Token:       fcmID,
			Priority:    "high",
			CollapseKey: "sync",
			Data:        map[string]interface{}{"msg": "sync"},
		}

		start := time.Now()
		_, err := fc.Send(sync)

		if err != nil {
			// log failures but continue, relayer will sync on its own
			logrus.WithError(err).WithField("channel_uuid", channel.UUID()).Error("error syncing channel")
		} else {
			logrus.WithField("elapsed", time.Since(start)).WithField("channel_uuid", channel.UUID()).Debug("android sync complete")
		}
	}
}
