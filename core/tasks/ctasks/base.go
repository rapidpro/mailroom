package ctasks

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	valkey "github.com/gomodule/redigo/redis"
	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
)

// Task is the interface for all contact tasks - tasks which operate on a single contact in real time
type Task interface {
	Type() string
	Perform(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, contact *models.Contact) error
}

var registeredTypes = map[string]func() Task{}

func RegisterType(name string, initFunc func() Task) {
	registeredTypes[name] = initFunc
}

func ReadTask(type_ string, data []byte) (Task, error) {
	fn := registeredTypes[type_]
	if fn == nil {
		return nil, fmt.Errorf("unknown task type: %s", type_)
	}

	t := fn()
	return t, json.Unmarshal(data, t)
}

// Payload wrapper for encoding a contact task
type Payload struct {
	Type       string          `json:"type"`
	Task       json.RawMessage `json:"task"`
	QueuedOn   time.Time       `json:"queued_on"`
	ErrorCount int             `json:"error_count,omitempty"`
}

// Queue adds a contact task to a contact's queue
func Queue(ctx context.Context, rt *runtime.Runtime, orgID models.OrgID, contactID models.ContactID, task Task, front bool, errorCount int) error {
	vc := rt.VK.Get()
	defer vc.Close()

	taskJSON, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("error marshalling contact task: %w", err)
	}

	payload := &Payload{Type: task.Type(), Task: taskJSON, QueuedOn: dates.Now(), ErrorCount: errorCount}
	payloadJSON := jsonx.MustMarshal(payload)

	// first push the event on our contact queue
	contactQ := fmt.Sprintf("c:%d:%d", orgID, contactID)
	if front {
		_, err = valkey.Int64(valkey.DoContext(vc, ctx, "LPUSH", contactQ, string(payloadJSON)))

	} else {
		_, err = valkey.Int64(valkey.DoContext(vc, ctx, "RPUSH", contactQ, string(payloadJSON)))
	}
	if err != nil {
		return fmt.Errorf("error queuing contact task: %w", err)
	}

	return nil
}

func Perform(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, contactID models.ContactID, task Task) error {
	contact, err := models.LoadContact(ctx, rt.DB, oa, contactID)
	if err != nil {
		if err == sql.ErrNoRows { // if contact no longer exists, ignore event, whatever it was gonna do is about to be deleted too
			return nil
		}
		return fmt.Errorf("error loading contact: %w", err)
	}

	return task.Perform(ctx, rt, oa, contact)
}
