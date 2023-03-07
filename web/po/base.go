package po

import (
	"context"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/pkg/errors"
)

var excludeProperties = []string{"arguments"}

func loadFlows(ctx context.Context, rt *runtime.Runtime, orgID models.OrgID, flowIDs []models.FlowID) ([]flows.Flow, error) {
	// grab our org assets
	oa, err := models.GetOrgAssets(ctx, rt, orgID)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to load org assets")
	}

	flows := make([]flows.Flow, len(flowIDs))
	for i, flowID := range flowIDs {
		dbFlow, err := oa.FlowByID(flowID)
		if err != nil {
			return nil, errors.Wrapf(err, "unable to load flow with ID %d", flowID)
		}

		flow, err := oa.SessionAssets().Flows().Get(dbFlow.UUID())
		if err != nil {
			return nil, errors.Wrapf(err, "unable to read flow with UUID %s", string(dbFlow.UUID()))
		}

		flows[i] = flow
	}
	return flows, nil
}
