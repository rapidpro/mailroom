package queues

import (
	"context"
	"encoding/json"
	"time"

	valkey "github.com/gomodule/redigo/redis"
	"github.com/nyaruka/vkutil/queues"
)

// Task is a wrapper for encoding a task
type Task struct {
	ID         queues.TaskID   `json:"-"`
	OwnerID    int             `json:"-"`
	Type       string          `json:"type"`
	Task       json.RawMessage `json:"task"`
	QueuedOn   time.Time       `json:"queued_on"`
	ErrorCount int             `json:"error_count,omitempty"`
}

// Fair is a queue that supports fair distribution of tasks between owners
type Fair interface {
	Push(ctx context.Context, vc valkey.Conn, taskType string, ownerID int, task any, priority bool) (queues.TaskID, error)
	Pop(ctx context.Context, vc valkey.Conn) (*Task, error)
	Done(ctx context.Context, vc valkey.Conn, ownerID int) error
	Pause(ctx context.Context, vc valkey.Conn, ownerID int) error
	Resume(ctx context.Context, vc valkey.Conn, ownerID int) error
	Queued(ctx context.Context, vc valkey.Conn) ([]int, error)
	Paused(ctx context.Context, vc valkey.Conn) ([]int, error)
	Size(ctx context.Context, vc valkey.Conn) (int, error)
	Dump(ctx context.Context, vc valkey.Conn) ([]byte, error)
}
