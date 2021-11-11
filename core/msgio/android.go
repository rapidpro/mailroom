package msgio

import (
	"time"

	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/pkg/errors"

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
		assert(channel.Type() == models.ChannelTypeAndroid, "can't sync a non-android channel")

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

// CreateFCMClient creates an FCM client based on the configured FCM API key
func CreateFCMClient(cfg *runtime.Config) *fcm.Client {
	if cfg.FCMKey == "" {
		return nil
	}
	client, err := fcm.NewClient(cfg.FCMKey)
	if err != nil {
		panic(errors.Wrap(err, "unable to create FCM client"))
	}
	return client
}
