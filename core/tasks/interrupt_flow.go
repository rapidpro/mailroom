package tasks

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
)

const (
	TypeInterruptFlow = "interrupt_flow"

	// valkey key prefix used to track the number of batches remaining to be interrupted for a flow
	interruptFlowProgressKey = "interrupt_flow_progress"
	interruptFlowProgressTTL = 15 * time.Minute
)

func init() {
	RegisterType(TypeInterruptFlow, func() Task { return &InterruptFlow{} })
}

// InterruptFlow is our task for interrupting all waiting sessions for a given flow. Since there could be many sessions,
// it creates batches of InterruptSessionBatch tasks to do the actual interrupting.
type InterruptFlow struct {
	FlowID models.FlowID `json:"flow_id" validate:"required"`
}

func (t *InterruptFlow) Type() string {
	return TypeInterruptFlow
}

// Timeout is the maximum amount of time the task can run for
func (t *InterruptFlow) Timeout() time.Duration {
	return 10 * time.Minute
}

func (t *InterruptFlow) WithAssets() models.Refresh {
	return models.RefreshNone
}

func (t *InterruptFlow) Perform(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets) error {
	sessionRefs, err := models.GetWaitingSessionsForFlow(ctx, rt.DB, t.FlowID)
	if err != nil {
		return fmt.Errorf("error getting waiting sessions for flow: %w", err)
	}

	counter := FlowInterruptCounter(t.FlowID)

	if len(sessionRefs) == 0 {
		if err := counter.Clear(ctx, rt.VK); err != nil {
			return fmt.Errorf("error clearing flow interrupt progress key: %w", err)
		}
		return nil
	}

	batches := slices.Collect(slices.Chunk(sessionRefs, interruptSessionBatchSize))

	if err := counter.Init(ctx, rt.VK, len(batches)); err != nil {
		return fmt.Errorf("error setting flow interrupt batches remaining key: %w", err)
	}

	for _, batch := range batches {
		task := &InterruptSessionBatch{Sessions: batch, Status: flows.SessionStatusInterrupted, FlowID: t.FlowID}

		if err := Queue(ctx, rt, rt.Queues.Batch, oa.OrgID(), task, false); err != nil {
			return fmt.Errorf("error queueing interrupt session batch task: %w", err)
		}
	}

	return nil
}

// FlowInterruptCounter returns a counter for tracking flow interruption progress for the given flow.
func FlowInterruptCounter(flowID models.FlowID) *Counter {
	return NewCounter(fmt.Sprintf("%s:%d", interruptFlowProgressKey, flowID), interruptFlowProgressTTL)
}
