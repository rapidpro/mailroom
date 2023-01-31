package tasks

import (
	"context"
	"encoding/json"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/queue"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/pkg/errors"
)

var registeredTypes = map[string](func() Task){}

// RegisterType registers a new type of task
func RegisterType(name string, initFunc func() Task) {
	registeredTypes[name] = initFunc

	mailroom.AddTaskFunction(name, Perform)
}

// Task is the common interface for all task types
type Task interface {
	Type() string

	// Timeout is the maximum amount of time the task can run for
	Timeout() time.Duration

	// Perform performs the task
	Perform(ctx context.Context, rt *runtime.Runtime, orgID models.OrgID) error
}

// Performs a raw task popped from a queue
func Perform(ctx context.Context, rt *runtime.Runtime, task *queue.Task) error {
	// decode our task body
	typedTask, err := ReadTask(task.Type, task.Task)
	if err != nil {
		return errors.Wrapf(err, "error reading task of type %s", task.Type)
	}

	ctx, cancel := context.WithTimeout(ctx, typedTask.Timeout())
	defer cancel()

	return typedTask.Perform(ctx, rt, models.OrgID(task.OrgID))
}

// Queue adds the given task to the named queue
func Queue(rc redis.Conn, qname string, orgID models.OrgID, task Task, priority queue.Priority) error {
	return queue.AddTask(rc, qname, task.Type(), int(orgID), task, priority)
}

//------------------------------------------------------------------------------------------
// JSON Encoding / Decoding
//------------------------------------------------------------------------------------------

// ReadTask reads an action from the given JSON
func ReadTask(typeName string, data json.RawMessage) (Task, error) {
	f := registeredTypes[typeName]
	if f == nil {
		return nil, errors.Errorf("unknown task type: '%s'", typeName)
	}

	task := f()
	return task, utils.UnmarshalAndValidate(data, task)
}
