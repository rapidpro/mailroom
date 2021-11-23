package hooks

import (
	"context"
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
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
			nodes[wc.NodeUUID] = true
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

			// node is unhealthy if it's made at least 10 unhealthy calls in last 20 minutes and unhealthy percentage is > 25%
			if totalUnhealthy >= 10 && (100*totalUnhealthy/(totalHealthy+totalUnhealthy)) > 25 {
				unhealthyNodes[node] = true
			}
		}
	}

	if len(unhealthyNodes) > 0 {
		incidentID, err := models.IncidentWebhooksUnhealthy(ctx, tx, oa)
		if err != nil {
			return errors.Wrap(err, "error creating unhealthy webhooks incident")
		}

		err = redisSADDNodes(rc, fmt.Sprintf("incident:%d:nodes", incidentID), unhealthyNodes, time.Hour)
		if err != nil {
			return errors.Wrap(err, "error recording nodes for webhook incident")
		}
	}

	return nil
}

// utility to bulk add node UUIDs to a redis set
func redisSADDNodes(rc redis.Conn, key string, uuids map[flows.NodeUUID]bool, expires time.Duration) error {
	args := make([]interface{}, len(uuids)+1)
	args[0] = key
	a := 1
	for uuid := range uuids {
		args[a] = uuid
		a++
	}
	rc.Send("MULTI")
	rc.Send("SADD", args...)
	rc.Send("EXPIRE", key, int64(expires/time.Second))
	_, err := rc.Do("EXEC")
	return err
}
