package hooks

import (
	"context"

	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/pkg/errors"
)

type WebhookCall struct {
	NodeUUID flows.NodeUUID
	Event    *events.WebhookCalledEvent
}

var MonitorWebhooks models.EventCommitHook = &monitorWebhooks{}

type monitorWebhooks struct{}

func (h *monitorWebhooks) Apply(ctx context.Context, rt *runtime.Runtime, tx *sqlx.Tx, oa *models.OrgAssets, scenes map[*models.Scene][]interface{}) error {
	// organize events by nodes
	eventsByNode := make(map[flows.NodeUUID][]*events.WebhookCalledEvent)
	for _, es := range scenes {
		for _, e := range es {
			wc := e.(*WebhookCall)
			eventsByNode[wc.NodeUUID] = append(eventsByNode[wc.NodeUUID], wc.Event)
		}
	}

	unhealthyNodeUUIDs := make([]flows.NodeUUID, 0, 10)

	// record events against each node and determine if it's healthy
	for nodeUUID, events := range eventsByNode {
		node := &models.WebhookNode{UUID: nodeUUID}
		if err := node.Record(rt, events); err != nil {
			return errors.Wrap(err, "error recording events for webhook node")
		}

		healthy, err := node.Healthy(rt)
		if err != nil {
			return errors.Wrap(err, "error getting health of webhook node")
		}

		if !healthy {
			unhealthyNodeUUIDs = append(unhealthyNodeUUIDs, nodeUUID)
		}
	}

	// if we have unhealthy nodes, ensure we have an incident
	if len(unhealthyNodeUUIDs) > 0 {
		_, err := models.IncidentWebhooksUnhealthy(ctx, tx, rt.RP, oa, unhealthyNodeUUIDs)
		if err != nil {
			return errors.Wrap(err, "error creating unhealthy webhooks incident")
		}
	}

	return nil
}
