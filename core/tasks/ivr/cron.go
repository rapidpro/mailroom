package ivr

import (
	"context"
	"time"

	"github.com/nyaruka/mailroom"
	"github.com/nyaruka/mailroom/core/ivr"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func init() {
	mailroom.RegisterCron("retry_ivr_calls", time.Minute, false, RetryCalls)
}

// RetryCalls looks for calls that need to be retried and retries them
func RetryCalls(ctx context.Context, rt *runtime.Runtime) error {
	log := logrus.WithField("comp", "ivr_cron_retryer")
	start := time.Now()

	// find all calls that need restarting
	ctx, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	calls, err := models.LoadCallsToRetry(ctx, rt.DB, 100)
	if err != nil {
		return errors.Wrapf(err, "error loading calls to retry")
	}

	throttledChannels := make(map[models.ChannelID]bool)
	clogs := make([]*models.ChannelLog, 0, len(calls))

	// schedules requests for each call
	for _, call := range calls {
		log = log.WithField("call_id", call.ID())

		// if the channel for this call is throttled, move on
		if throttledChannels[call.ChannelID()] {
			call.MarkThrottled(ctx, rt.DB, time.Now())
			log.WithField("channel_id", call.ChannelID()).Info("skipping call, throttled")
			continue
		}

		// load the org for this call
		oa, err := models.GetOrgAssets(ctx, rt, call.OrgID())
		if err != nil {
			log.WithError(err).WithField("org_id", call.OrgID()).Error("error loading org")
			continue
		}

		// and the associated channel
		channel := oa.ChannelByID(call.ChannelID())
		if channel == nil {
			// fail this call, channel is no longer active
			err = models.BulkUpdateCallStatuses(ctx, rt.DB, []models.CallID{call.ID()}, models.CallStatusFailed)
			if err != nil {
				log.WithError(err).WithField("channel_id", call.ChannelID()).Error("error marking call as failed due to missing channel")
			}
			continue
		}

		// finally load the full URN
		urn, err := models.URNForID(ctx, rt.DB, oa, call.ContactURNID())
		if err != nil {
			log.WithError(err).WithField("urn_id", call.ContactURNID()).Error("unable to load contact urn")
			continue
		}

		clog, err := ivr.RequestStartForCall(ctx, rt, channel, urn, call)
		if clog != nil {
			clogs = append(clogs, clog)
		}
		if err != nil {
			log.WithError(err).Error(err)
			continue
		}

		// queued status on a call we just tried means it is throttled, mark our channel as such
		throttledChannels[call.ChannelID()] = true
	}

	// log any error inserting our channel logs, but continue
	if err := models.InsertChannelLogs(ctx, rt.DB, clogs); err != nil {
		logrus.WithError(err).Error("error inserting channel logs")
	}

	log.WithField("count", len(calls)).WithField("elapsed", time.Since(start)).Info("retried errored calls")

	return nil
}
