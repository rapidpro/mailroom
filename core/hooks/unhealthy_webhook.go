package hooks

import (
	"context"
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/pkg/errors"
)

var UnhealthyWebhooks models.EventCommitHook = &unhealthyWebhooks{}

type unhealthyWebhooks struct{}

func (h *unhealthyWebhooks) Apply(ctx context.Context, rt *runtime.Runtime, tx *sqlx.Tx, oa *models.OrgAssets, scenes map[*models.Scene][]interface{}) error {
	rc := rt.RP.Get()
	defer rc.Close()

	// extract set of unique node UUIDs
	nodesSeen := make(map[flows.NodeUUID]bool)
	nodes := make([]flows.NodeUUID, 0, 5)
	for _, es := range scenes {
		for _, e := range es {
			n := e.(flows.NodeUUID)
			if !nodesSeen[n] {
				nodesSeen[n] = true
				nodes = append(nodes, n)
			}
		}
	}

	unhealthyStartKey := fmt.Sprintf("webhooks:unhealthy:%d:start", oa.OrgID())
	unhealthyNodesKey := fmt.Sprintf("webhooks:unhealthy:%d:nodes", oa.OrgID())

	// set the key for when this org entered the unhealthy state, or get back the existing start time value
	unhealthyStartMS, err := redis.Int64(rc.Do("SET", unhealthyStartKey, dates.Now().UnixMilli(), "EX", 60*5, "NX", "GET"))
	if err != nil && err != redis.ErrNil {
		return errors.Wrap(err, "error setting webhooks unhealthy start in redis")
	}

	// add these node UUIDs to the set of unhealthy nodes
	_, err = sadd(rc, unhealthyNodesKey, nodes)
	if err != nil {
		return errors.Wrap(err, "error setting webhooks unhealthy start in redis")
	}

	if unhealthyStartMS != 0 {
		unhealthyStart := time.UnixMilli(unhealthyStartMS)
		unhealthySince := dates.Now().Sub(unhealthyStart)

		// if we've been unhealthy for at least 20 minutes, create an incident
		if unhealthySince > 20*time.Minute {
			err = models.GetOrCreateWebhooksUnhealthyIncident(ctx, tx, oa)
			if err != nil {
				return errors.Wrap(err, "error creating unhealthy webhooks incident")
			}
		}
	}

	return nil
}

// helper to add node UUIDs to a redis set in bulk
func sadd(rc redis.Conn, key string, uuids []flows.NodeUUID) (int, error) {
	args := make([]interface{}, len(uuids)+1)
	args[0] = key
	for i := range uuids {
		args[i+1] = uuids[i]
	}
	return redis.Int(rc.Do("SADD", args...))
}
