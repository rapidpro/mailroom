package flow

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/web"
	"github.com/nyaruka/vkutil/locks"
)

func init() {
	web.InternalRoute(http.MethodPost, "/flow/interrupt", web.JSONPayload(handleInterrupt))
}

// Request that sessions using the given flow are interrupted. Used as part of flow archival.
//
//	{
//	  "org_id": 1,
//	  "flow_id": 235
//	}
type interruptRequest struct {
	OrgID  models.OrgID  `json:"org_id"  validate:"required"`
	FlowID models.FlowID `json:"flow_id" validate:"required"`
}

func handleInterrupt(ctx context.Context, rt *runtime.Runtime, r *interruptRequest) (any, int, error) {
	locker := locks.NewLocker(fmt.Sprintf("flow_interrupt:%d", r.FlowID), time.Second*30)
	lock, err := locker.Grab(ctx, rt.VK, time.Second*5)
	if err != nil {
		return nil, 0, fmt.Errorf("error grabbing lock for flow interruption: %w", err)
	}
	if lock == "" {
		return nil, 0, fmt.Errorf("timeout waiting for flow interruption lock")
	}
	defer locker.Release(ctx, rt.VK, lock)

	// check if there is already an interruption in progress for this flow
	counter := tasks.FlowInterruptCounter(r.FlowID)
	remaining, err := counter.Value(ctx, rt.VK)
	if err != nil {
		return nil, 0, fmt.Errorf("error checking flow interrupt progress: %w", err)
	}
	if remaining != 0 {
		return map[string]any{"interrupted": false}, http.StatusOK, nil
	}

	// to avoid a race condition between checking for an existing interruption and setting the progress key in the task
	// below we set the progress key here with a placeholder value.
	if err := counter.Init(ctx, rt.VK, -1); err != nil {
		return nil, 0, fmt.Errorf("error initializing flow interrupt progress: %w", err)
	}

	task := &tasks.InterruptFlow{FlowID: r.FlowID}
	if err := tasks.Queue(ctx, rt, rt.Queues.Batch, r.OrgID, task, true); err != nil {
		return nil, 0, fmt.Errorf("error queuing interrupt flow task: %w", err)
	}

	return map[string]any{"interrupted": true}, http.StatusOK, nil
}
