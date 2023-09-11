package msgio

import (
	"time"

	"github.com/edganiukov/fcm"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// SyncAndroidChannel tries to trigger sync of the given Android channel via FCM
func SyncAndroidChannel(fc *fcm.Client, channel *models.Channel) error {
	if fc == nil {
		return errors.New("instance has no FCM configuration")
	}

	assert(channel.Type() == models.ChannelTypeAndroid, "can't sync a non-android channel")

	// no FCM ID for this channel, noop, we can't trigger a sync
	fcmID := channel.ConfigValue(models.ChannelConfigFCMID, "")
	if fcmID == "" {
		return nil
	}

	sync := &fcm.Message{
		Token:       fcmID,
		Priority:    "high",
		CollapseKey: "sync",
		Data:        map[string]any{"msg": "sync"},
	}

	start := time.Now()

	if _, err := fc.Send(sync); err != nil {
		return errors.Wrap(err, "error syncing channel")
	}

	logrus.WithField("elapsed", time.Since(start)).WithField("channel_uuid", channel.UUID()).Debug("android sync complete")
	return nil
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
