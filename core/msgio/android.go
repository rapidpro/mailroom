package msgio

import (
	"sync"
	"time"

	"github.com/nyaruka/mailroom/config"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/pkg/errors"

	"github.com/edganiukov/fcm"
	"github.com/sirupsen/logrus"
)

var clientInit sync.Once
var fcmClient *fcm.Client

func init() {
	clientInit.Do(func() {
		if config.Mailroom.FCMKey == "" {
			logrus.Error("fcm not configured, no syncing of android channels")
			return
		}

		var err error
		fcmClient, err = fcm.NewClient(config.Mailroom.FCMKey)
		if err != nil {
			panic(errors.Wrap(err, "unable to create FCM client"))
		}
	})
}

// SyncAndroidChannels tries to trigger syncs of the given Android channels via FCM
func SyncAndroidChannels(channels []*models.Channel) {
	if fcmClient == nil {
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
		_, err := fcmClient.Send(sync)

		if err != nil {
			// log failures but continue, relayer will sync on its own
			logrus.WithError(err).WithField("channel_uuid", channel.UUID()).Error("error syncing channel")
		} else {
			logrus.WithField("elapsed", time.Since(start)).WithField("channel_uuid", channel.UUID()).Debug("android sync complete")
		}
	}
}

// SetFCMClient sets the FCM client. Used for testing.
func SetFCMClient(client *fcm.Client) {
	fcmClient = client
}
