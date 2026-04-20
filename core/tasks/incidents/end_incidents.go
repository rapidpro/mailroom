package incidents

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/pkg/errors"
)

func init() {
	tasks.RegisterCron("end_incidents", false, &EndIncidentsCron{})
}

type EndIncidentsCron struct{}

func (c *EndIncidentsCron) Next(last time.Time) time.Time {
	return tasks.CronNext(last, time.Minute*3)
}

// EndIncidents checks open incidents and end any that no longer apply
func (c *EndIncidentsCron) Run(ctx context.Context, rt *runtime.Runtime) (map[string]any, error) {
	incidents, err := models.GetOpenIncidents(ctx, rt.DB, []models.IncidentType{models.IncidentTypeWebhooksUnhealthy})
	if err != nil {
		return nil, errors.Wrap(err, "error fetching open incidents")
	}

	numEnded := 0

	for _, incident := range incidents {
		if incident.Type == models.IncidentTypeWebhooksUnhealthy {
			ended, err := checkWebhookIncident(ctx, rt, incident)
			if err != nil {
				return nil, errors.Wrapf(err, "error checking webhook incident #%d", incident.ID)
			}
			if ended {
				numEnded++
			}
		}
	}

	return map[string]any{"ended": numEnded}, nil
}

func checkWebhookIncident(ctx context.Context, rt *runtime.Runtime, incident *models.Incident) (bool, error) {
	nodeUUIDs, err := getWebhookIncidentNodes(rt, incident)

	if err != nil {
		return false, errors.Wrap(err, "error getting webhook nodes")
	}

	healthyNodeUUIDs := make([]flows.NodeUUID, 0, len(nodeUUIDs))

	for _, nodeUUID := range nodeUUIDs {
		node := models.WebhookNode{UUID: flows.NodeUUID(nodeUUID)}
		healthy, err := node.Healthy(rt)
		if err != nil {
			return false, errors.Wrap(err, "error getting health of webhook nodes")
		}

		if healthy {
			healthyNodeUUIDs = append(healthyNodeUUIDs, nodeUUID)
		}
	}

	if len(healthyNodeUUIDs) > 0 {
		if err := removeWebhookIncidentNodes(rt, incident, healthyNodeUUIDs); err != nil {
			return false, errors.Wrap(err, "error removing nodes from webhook incident")
		}
	}

	log := slog.With("incident_id", incident.ID, "unhealthy", len(nodeUUIDs)-len(healthyNodeUUIDs), "healthy", len(healthyNodeUUIDs))

	// if all of the nodes are now healthy the incident has ended
	if len(healthyNodeUUIDs) == len(nodeUUIDs) {
		if err := incident.End(ctx, rt.DB); err != nil {
			return false, errors.Wrap(err, "error ending incident")
		}
		log.Info("ended webhook incident")
		return true, nil
	}

	log.Debug("checked webhook incident")
	return false, nil
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
