package contact

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/web"
)

func init() {
	web.InternalRoute(http.MethodPost, "/contact/populate_group", web.JSONPayload(handlePopulateGroup))
}

// Triggers population of a query based group in a task.
//
//	{
//	  "org_id": 1,
//	  "group_id": 3
//	}
type populateGroupRequest struct {
	OrgID   models.OrgID   `json:"org_id"  validate:"required"`
	GroupID models.GroupID `json:"group_id" validate:"required"`
}

// handles a request to populate a query based contact group
func handlePopulateGroup(ctx context.Context, rt *runtime.Runtime, r *populateGroupRequest) (any, int, error) {
	oa, err := models.GetOrgAssetsWithRefresh(ctx, rt, r.OrgID, models.RefreshGroups)
	if err != nil {
		return nil, 0, fmt.Errorf("error loading org assets: %w", err)
	}

	group := oa.GroupByID(r.GroupID)
	if group == nil || group.Query() == "" {
		return errors.New("no such query based group"), http.StatusBadRequest, nil
	}

	task := &tasks.PopulateGroup{
		GroupID: group.ID(),
		Query:   group.Query(),
	}

	if err := tasks.Queue(ctx, rt, rt.Queues.Batch, r.OrgID, task, true); err != nil {
		return nil, 0, fmt.Errorf("error queuing populate group task: %w", err)
	}

	return map[string]any{}, http.StatusOK, nil
}
