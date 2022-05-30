package incidents

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/utils/cron"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func init() {
	mailroom.AddInitFunction(startEndCron)
}

func startEndCron(rt *runtime.Runtime, wg *sync.WaitGroup, quit chan bool) error {
	cron.Start(quit, rt, "end_incidents", time.Minute*3, false,
		func() error {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
			defer cancel()
			return EndIncidents(ctx, rt)
		},
	)
	return nil
}

// EndIncidents checks open incidents and end any that no longer apply
func EndIncidents(ctx context.Context, rt *runtime.Runtime) error {
	incidents, err := models.GetOpenIncidents(ctx, rt.DB, []models.IncidentType{models.IncidentTypeWebhooksUnhealthy})
	if err != nil {
		return errors.Wrap(err, "error fetching open incidents")
	}

	for _, incident := range incidents {
		if incident.Type == models.IncidentTypeWebhooksUnhealthy {
			if err := checkWebhookIncident(ctx, rt, incident); err != nil {
				return errors.Wrapf(err, "error checking webhook incident #%d", incident.ID)
			}
		}
	}

	return nil
}

func checkWebhookIncident(ctx context.Context, rt *runtime.Runtime, incident *models.Incident) error {
	nodeUUIDs, err := getWebhookIncidentNodes(rt, incident)

	if err != nil {
		return errors.Wrap(err, "error getting webhook nodes")
	}

	healthyNodeUUIDs := make([]flows.NodeUUID, 0, len(nodeUUIDs))

	for _, nodeUUID := range nodeUUIDs {
		node := models.WebhookNode{UUID: flows.NodeUUID(nodeUUID)}
		healthy, err := node.Healthy(rt)
		if err != nil {
			return errors.Wrap(err, "error getting health of webhook nodes")
		}

		if healthy {
			healthyNodeUUIDs = append(healthyNodeUUIDs, nodeUUID)
		}
	}

	if len(healthyNodeUUIDs) > 0 {
		if err := removeWebhookIncidentNodes(rt, incident, healthyNodeUUIDs); err != nil {
			return errors.Wrap(err, "error removing nodes from webhook incident")
		}
	}

	log := logrus.WithFields(logrus.Fields{"incident_id": incident.ID, "unhealthy": len(nodeUUIDs) - len(healthyNodeUUIDs), "healthy": len(healthyNodeUUIDs)})

	// if all of the nodes are now healthy the incident has ended
	if len(healthyNodeUUIDs) == len(nodeUUIDs) {
		if err := incident.End(ctx, rt.DB); err != nil {
			return errors.Wrap(err, "error ending incident")
		}
		log.Info("ended webhook incident")
	} else {
		log.Debug("checked webhook incident")
	}

	return nil
}

func getWebhookIncidentNodes(rt *runtime.Runtime, incident *models.Incident) ([]flows.NodeUUID, error) {
	rc := rt.RP.Get()
	defer rc.Close()

	nodesKey := fmt.Sprintf("incident:%d:nodes", incident.ID)
	nodes, err := redis.Strings(rc.Do("SMEMBERS", nodesKey))
	if err != nil {
		return nil, err
	}

	nodeUUIDs := make([]flows.NodeUUID, len(nodes))
	for i := range nodes {
		nodeUUIDs[i] = flows.NodeUUID(nodes[i])
	}
	return nodeUUIDs, nil
}

func removeWebhookIncidentNodes(rt *runtime.Runtime, incident *models.Incident, nodes []flows.NodeUUID) error {
	rc := rt.RP.Get()
	defer rc.Close()

	nodesKey := fmt.Sprintf("incident:%d:nodes", incident.ID)
	_, err := rc.Do("SREM", redis.Args{}.Add(nodesKey).AddFlat(nodes)...)
	if err != nil {
		return err
	}
	return nil
}
