package crons

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	valkey "github.com/gomodule/redigo/redis"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
)

func init() {
	Register("end_incidents", &EndIncidentsCron{})
}

type EndIncidentsCron struct{}

func (c *EndIncidentsCron) Next(last time.Time) time.Time {
	return Next(last, time.Minute*3)
}

func (c *EndIncidentsCron) AllInstances() bool {
	return false
}

// EndIncidents checks open incidents and end any that no longer apply
func (c *EndIncidentsCron) Run(ctx context.Context, rt *runtime.Runtime) (map[string]any, error) {
	incidents, err := models.GetOpenIncidents(ctx, rt.DB, []models.IncidentType{models.IncidentTypeWebhooksUnhealthy})
	if err != nil {
		return nil, fmt.Errorf("error fetching open incidents: %w", err)
	}

	numEnded := 0

	for _, incident := range incidents {
		if incident.Type == models.IncidentTypeWebhooksUnhealthy {
			ended, err := c.checkWebhookIncident(ctx, rt, incident)
			if err != nil {
				return nil, fmt.Errorf("error checking webhook incident #%d: %w", incident.ID, err)
			}
			if ended {
				numEnded++
			}
		}
	}

	return map[string]any{"ended": numEnded}, nil
}

func (c *EndIncidentsCron) checkWebhookIncident(ctx context.Context, rt *runtime.Runtime, incident *models.Incident) (bool, error) {
	nodeUUIDs, err := c.getWebhookIncidentNodes(rt, incident)

	if err != nil {
		return false, fmt.Errorf("error getting webhook nodes: %w", err)
	}

	healthyNodeUUIDs := make([]flows.NodeUUID, 0, len(nodeUUIDs))

	for _, nodeUUID := range nodeUUIDs {
		node := models.WebhookNode{UUID: flows.NodeUUID(nodeUUID)}
		healthy, err := node.Healthy(ctx, rt)
		if err != nil {
			return false, fmt.Errorf("error getting health of webhook nodes: %w", err)
		}

		if healthy {
			healthyNodeUUIDs = append(healthyNodeUUIDs, nodeUUID)
		}
	}

	if len(healthyNodeUUIDs) > 0 {
		if err := c.removeWebhookIncidentNodes(rt, incident, healthyNodeUUIDs); err != nil {
			return false, fmt.Errorf("error removing nodes from webhook incident: %w", err)
		}
	}

	log := slog.With("incident_id", incident.ID, "unhealthy", len(nodeUUIDs)-len(healthyNodeUUIDs), "healthy", len(healthyNodeUUIDs))

	// if all of the nodes are now healthy the incident has ended
	if len(healthyNodeUUIDs) == len(nodeUUIDs) {
		if err := incident.End(ctx, rt.DB); err != nil {
			return false, fmt.Errorf("error ending incident: %w", err)
		}
		log.Info("ended webhook incident")
		return true, nil
	}

	log.Debug("checked webhook incident")
	return false, nil
}

func (c *EndIncidentsCron) getWebhookIncidentNodes(rt *runtime.Runtime, incident *models.Incident) ([]flows.NodeUUID, error) {
	vc := rt.VK.Get()
	defer vc.Close()

	nodesKey := fmt.Sprintf("incident:%d:nodes", incident.ID)
	nodes, err := valkey.Strings(vc.Do("SMEMBERS", nodesKey))
	if err != nil {
		return nil, err
	}

	nodeUUIDs := make([]flows.NodeUUID, len(nodes))
	for i := range nodes {
		nodeUUIDs[i] = flows.NodeUUID(nodes[i])
	}
	return nodeUUIDs, nil
}

func (c *EndIncidentsCron) removeWebhookIncidentNodes(rt *runtime.Runtime, incident *models.Incident, nodes []flows.NodeUUID) error {
	vc := rt.VK.Get()
	defer vc.Close()

	nodesKey := fmt.Sprintf("incident:%d:nodes", incident.ID)
	_, err := vc.Do("SREM", valkey.Args{}.Add(nodesKey).AddFlat(nodes)...)
	if err != nil {
		return err
	}
	return nil
}
