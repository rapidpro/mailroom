package hooks

import (
	"context"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/utils/redisx"
	"github.com/pkg/errors"
)

type WebhookCall struct {
	NodeUUID flows.NodeUUID
	Elapsed  time.Duration
}

var MonitorWebhooks models.EventCommitHook = &monitorWebhooks{}

type monitorWebhooks struct{}

func (h *monitorWebhooks) Apply(ctx context.Context, rt *runtime.Runtime, tx *sqlx.Tx, oa *models.OrgAssets, scenes map[*models.Scene][]interface{}) error {
	healthyLimit := time.Duration(rt.Config.WebhooksHealthyResponseLimit) * time.Millisecond

	nodes := make(map[flows.NodeUUID]bool)
	healthyByNode := make(map[flows.NodeUUID]int)
	unhealthyByNode := make(map[flows.NodeUUID]int)

	for _, es := range scenes {
		for _, e := range es {
			wc := e.(*WebhookCall)
			if wc.Elapsed <= healthyLimit {
				healthyByNode[wc.NodeUUID]++
			} else {
				unhealthyByNode[wc.NodeUUID]++
			}
		}
	}

	rc := rt.RP.Get()
	defer rc.Close()

	unhealthyNodes := make(map[flows.NodeUUID]bool)

	for node := range nodes {
		tracker := redisx.NewStatesTracker(fmt.Sprintf("webhook:%s", node), []string{"healthy", "unhealthy"}, time.Minute*5, time.Minute*20)
		if healthyByNode[node] > 0 {
			tracker.Record(rc, "healthy", healthyByNode[node])
		}
		if unhealthyByNode[node] > 0 {
			tracker.Record(rc, "unhealthy", unhealthyByNode[node])

			// if we're recording unhealthy calls for this node, check on its current state
			totals, _ := tracker.Current(rc)
			totalHealthy, totalUnhealthy := totals["healthy"], totals["unhealthy"]
			if totalUnhealthy > 10 && totalUnhealthy > totalHealthy { // TODO
				unhealthyNodes[node] = true
			}
		}
	}

	if len(unhealthyNodes) > 0 {
		err := models.IncidentWebhooksUnhealthy(ctx, tx, oa)
		if err != nil {
			return errors.Wrap(err, "error creating unhealthy webhooks incident")
		}
	}

	return nil
}
