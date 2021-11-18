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

const (
	unhealthyResolvedAfter        = 5 * time.Minute
	unhealthyBecomesIncidentAfter = 20 * time.Minute
)

var UnhealthyWebhooks models.EventCommitHook = &unhealthyWebhooks{}

type unhealthyWebhooks struct{}

func (h *unhealthyWebhooks) Apply(ctx context.Context, rt *runtime.Runtime, tx *sqlx.Tx, oa *models.OrgAssets, scenes map[*models.Scene][]interface{}) error {
	rc := rt.RP.Get()
	defer rc.Close()

	// count unhealthy calls and extract set of unique node UUIDs
	count := 0
	nodesSeen := make(map[flows.NodeUUID]bool)
	nodes := make([]flows.NodeUUID, 0, 5)
	for _, es := range scenes {
		for _, e := range es {
			n := e.(flows.NodeUUID)
			if !nodesSeen[n] {
				nodesSeen[n] = true
				nodes = append(nodes, n)
			}
			count++
		}
	}

	unhealthyStartKey := fmt.Sprintf("webhooks:unhealthy:%d:start", oa.OrgID())
	unhealthyCountKey := fmt.Sprintf("webhooks:unhealthy:%d:count", oa.OrgID())
	unhealthyNodesKey := fmt.Sprintf("webhooks:unhealthy:%d:nodes", oa.OrgID())
	expireKeysSeconds := int(unhealthyResolvedAfter / time.Second)

	// set the key for when this org entered the unhealthy state if not already set
	_, err := rc.Do("SET", unhealthyStartKey, dates.Now().Unix(), "NX", "EX", expireKeysSeconds)
	if err != nil && err != redis.ErrNil {
		return errors.Wrap(err, "error setting webhooks unhealthy start in redis")
	}

	unhealthyStartUnix, err := redis.Int64(rc.Do("GET", unhealthyStartKey))
	if err != nil {
		return errors.Wrap(err, "error setting webhooks unhealthy start in redis")
	}

	rc.Do("INCRBY", unhealthyCountKey, count)
	rc.Do("EXPIRE", unhealthyCountKey, expireKeysSeconds)

	// add these node UUIDs to the set of unhealthy nodes
	_, err = sadd(rc, unhealthyNodesKey, nodes)
	if err != nil {
		return errors.Wrap(err, "error setting webhooks unhealthy start in redis")
	}

	rc.Do("EXPIRE", unhealthyNodesKey, expireKeysSeconds)

	if unhealthyStartUnix != 0 {
		unhealthyStart := time.Unix(unhealthyStartUnix, 0)
		unhealthySince := dates.Now().Sub(unhealthyStart)

		// if we've been unhealthy for too long, create an incident
		if unhealthySince > unhealthyBecomesIncidentAfter {
			err = models.IncidentWebhooksUnhealthy(ctx, tx, oa)
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
