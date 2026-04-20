package ivr

import (
	"context"
	"log/slog"
	"time"

	"github.com/nyaruka/mailroom/core/ivr"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/pkg/errors"
)

func init() {
	tasks.RegisterCron("retry_ivr_calls", false, &RetryCron{})
}

type RetryCron struct{}

func (c *RetryCron) Next(last time.Time) time.Time {
	return tasks.CronNext(last, time.Minute)
}

// RetryCalls looks for calls that need to be retried and retries them
func (c *RetryCron) Run(ctx context.Context, rt *runtime.Runtime) (map[string]any, error) {
	log := slog.With("comp", "ivr_cron_retryer")

	// find all calls that need restarting
	ctx, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	calls, err := models.LoadCallsToRetry(ctx, rt.DB, 100)
	if err != nil {
		return nil, errors.Wrapf(err, "error loading calls to retry")
	}

	throttledChannels := make(map[models.ChannelID]bool)
	clogs := make([]*models.ChannelLog, 0, len(calls))

	// schedules requests for each call
	for _, call := range calls {
		log = log.With("call_id", call.ID())

		// if the channel for this call is throttled, move on
		if throttledChannels[call.ChannelID()] {
			call.MarkThrottled(ctx, rt.DB, time.Now())
			log.Info("skipping call, throttled", "channel_id", call.ChannelID())
			continue
		}

		// load the org for this call
		oa, err := models.GetOrgAssets(ctx, rt, call.OrgID())
		if err != nil {
			log.Error("error loading org", "error", err, "org_id", call.OrgID())
			continue
		}

		// and the associated channel
		channel := oa.ChannelByID(call.ChannelID())
		if channel == nil {
			// fail this call, channel is no longer active
			err = models.BulkUpdateCallStatuses(ctx, rt.DB, []models.CallID{call.ID()}, models.CallStatusFailed)
			if err != nil {
				log.Error("error marking call as failed due to missing channel", "error", err, "channel_id", call.ChannelID())
			}
			continue
		}

		// finally load the full URN
		urn, err := models.URNForID(ctx, rt.DB, oa, call.ContactURNID())
		if err != nil {
			log.Error("unable to load contact urn", "error", err, "urn_id", call.ContactURNID())
			continue
		}

		clog, err := ivr.RequestStartForCall(ctx, rt, channel, urn, call)
		if clog != nil {
			clogs = append(clogs, clog)
		}
		if err != nil {
			log.Error("error requesting start for call", "error", err)
			continue
		}

		// queued status on a call we just tried means it is throttled, mark our channel as such
		throttledChannels[call.ChannelID()] = true
	}

	// log any error inserting our channel logs, but continue
	if err := models.InsertChannelLogs(ctx, rt, clogs); err != nil {
		slog.Error("error inserting channel logs", "error", err)
	}

	return map[string]any{"retried": len(calls)}, nil
}
