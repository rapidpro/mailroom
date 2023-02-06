package starts

import (
	"context"
	"time"

	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/runner"
	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/pkg/errors"
)

const TypeStartFlowBatch = "start_flow_batch"

func init() {
	tasks.RegisterType(TypeStartFlowBatch, func() tasks.Task { return &StartFlowBatchTask{} })
}

// StartFlowBatchTask is the start flow batch task
type StartFlowBatchTask struct {
	*models.FlowStartBatch
}

func (t *StartFlowBatchTask) Type() string {
	return TypeStartFlowBatch
}

// Timeout is the maximum amount of time the task can run for
func (t *StartFlowBatchTask) Timeout() time.Duration {
	return time.Minute * 15
}

func (t *StartFlowBatchTask) Perform(ctx context.Context, rt *runtime.Runtime, orgID models.OrgID) error {
	// start these contacts in our flow
	_, err := runner.StartFlowBatch(ctx, rt, t.FlowStartBatch)
	if err != nil {
		return errors.Wrap(err, "error starting flow batch")
	}
	return nil
}
