package queues

import (
	"context"
	"fmt"
	"strconv"

	valkey "github.com/gomodule/redigo/redis"
	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/vkutil/queues"
)

type FairV2 struct {
	name string
	base queues.Fair
}

func NewFair(name string, maxActivePerOwner int) *FairV2 {
	return &FairV2{
		name: name,
		base: *queues.NewFair(fmt.Sprintf("tasks:%s", name), maxActivePerOwner),
	}
}

func (q *FairV2) String() string {
	return q.name
}

func (q *FairV2) Push(ctx context.Context, vc valkey.Conn, taskType string, ownerID int, task any, priority bool) (queues.TaskID, error) {
	taskJSON := jsonx.MustMarshal(task)

	wrapper := &Task{Type: taskType, OwnerID: ownerID, Task: taskJSON, QueuedOn: dates.Now()}
	raw := jsonx.MustMarshal(wrapper)

	return q.base.Push(ctx, vc, queues.OwnerID(fmt.Sprint(ownerID)), priority, raw)
}

func (q *FairV2) Pop(ctx context.Context, vc valkey.Conn) (*Task, error) {
	taskID, ownerID, raw, err := q.base.Pop(ctx, vc)
	if err != nil {
		return nil, fmt.Errorf("error popping task: %w", err)
	}

	if ownerID == "" || raw == nil {
		return nil, nil // no task available
	}

	task := &Task{}
	if err := jsonx.Unmarshal(raw, task); err != nil {
		return nil, fmt.Errorf("error unmarshaling task %s: %w", taskID, err)
	}

	task.ID = taskID
	task.OwnerID, _ = strconv.Atoi(string(ownerID))

	return task, nil
}

func (q *FairV2) Done(ctx context.Context, vc valkey.Conn, ownerID int) error {
	return q.base.Done(ctx, vc, queues.OwnerID(fmt.Sprint(ownerID)))
}

func (q *FairV2) Queued(ctx context.Context, vc valkey.Conn) ([]int, error) {
	strs, err := q.base.Queued(ctx, vc)
	if err != nil {
		return nil, err
	}

	actual := make([]int, len(strs))
	for i, s := range strs {
		owner, _ := strconv.ParseInt(string(s), 10, 64)
		actual[i] = int(owner)
	}

	return actual, nil
}

func (q *FairV2) Paused(ctx context.Context, vc valkey.Conn) ([]int, error) {
	strs, err := q.base.Paused(ctx, vc)
	if err != nil {
		return nil, err
	}

	actual := make([]int, len(strs))
	for i, s := range strs {
		owner, _ := strconv.ParseInt(string(s), 10, 64)
		actual[i] = int(owner)
	}

	return actual, nil
}

func (q *FairV2) Size(ctx context.Context, vc valkey.Conn) (int, error) {
	owners, err := q.base.Queued(ctx, vc)
	if err != nil {
		return 0, fmt.Errorf("error getting queued task owners: %w", err)
	}

	total := 0
	for _, owner := range owners {
		size, err := q.base.Size(ctx, vc, owner)
		if err != nil {
			return 0, fmt.Errorf("error getting size for owner %s: %w", owner, err)
		}
		total += size
	}

	return total, nil
}

func (q *FairV2) Pause(ctx context.Context, vc valkey.Conn, ownerID int) error {
	return q.base.Pause(ctx, vc, queues.OwnerID(fmt.Sprint(ownerID)))
}

func (q *FairV2) Resume(ctx context.Context, vc valkey.Conn, ownerID int) error {
	return q.base.Resume(ctx, vc, queues.OwnerID(fmt.Sprint(ownerID)))
}

func (q *FairV2) Dump(ctx context.Context, vc valkey.Conn) ([]byte, error) {
	return q.base.Dump(ctx, vc)
}

var _ Fair = (*FairV2)(nil)
