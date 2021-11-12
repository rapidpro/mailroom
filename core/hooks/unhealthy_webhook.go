package hooks

import (
	"context"

	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
)

type UnhealthyWebhook struct {
	Flow     *models.Flow
	NodeUUID flows.NodeUUID
}

var UnhealthyWebhooks models.EventCommitHook = &unhealthyWebhooks{}

type unhealthyWebhooks struct{}

func (h *unhealthyWebhooks) Apply(ctx context.Context, rt *runtime.Runtime, tx *sqlx.Tx, oa *models.OrgAssets, scenes map[*models.Scene][]interface{}) error {

	// find unique flows and nodes

	return nil
}

/*func handleUnhealthyWebhook(rt *runtime.Runtime, flow *models.Flow, nodeUUID flows.NodeUUID) error {
	rc := rt.RP.Get()
	defer rc.Close()

	unhealthyStartKey := fmt.Sprintf("webhook:%s:unhealthy:start", nodeUUID)

	unhealthyStartMS, err := redis.Int64(rc.Do("SET", unhealthyStartKey, dates.Now().UnixMilli(), "EX", 60*5, "NX", "GET"))
	if err != nil && err != redis.ErrNil {
		return errors.Wrap(err, "error setting webhook unhealthy start in redis")
	}

	if unhealthyStartMS != 0 {
		unhealthyStart := time.UnixMilli(unhealthyStartMS)
		unhealthySince := dates.Now().Sub(unhealthyStart)

		// TODO create incident
		fmt.Println(unhealthySince)
	}

	return nil
}*/
