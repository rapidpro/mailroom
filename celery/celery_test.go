package celery

import (
	"encoding/json"
	"testing"

	"github.com/gomodule/redigo/redis"
	"github.com/nyaruka/mailroom/testsuite"
)

func TestQueue(t *testing.T) {
	testsuite.ResetRP()
	rc := testsuite.RC()
	defer rc.Close()

	// queue to our handler queue
	rc.Send("multi")
	err := QueueTask(rc, "handler", "handle_event_task", []int64{})
	if err != nil {
		t.Error(err)
	}
	_, err = rc.Do("exec")
	if err != nil {
		t.Error(err)
	}

	// check whether things look right
	taskJSON, err := redis.String(rc.Do("LPOP", "handler"))
	if err != nil {
		t.Errorf("should have value in handler queue: %s", err)
	}

	// make sure our task is valid json
	task := Task{}
	err = json.Unmarshal([]byte(taskJSON), &task)
	if err != nil {
		t.Errorf("should be JSON: %s", err)
	}

	// and is against the right queue
	if task.Properties.DeliveryInfo.RoutingKey != "handler" {
		t.Errorf("task should have handler as routing key")
	}
}
