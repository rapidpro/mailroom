package queue

import (
	"encoding/json"
	"testing"

	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/assert"
)

func TestQueues(t *testing.T) {
	rc, err := redis.Dial("tcp", "localhost:6379")
	assert.NoError(t, err)
	rc.Do("del", "test:active", "test:1", "test:2", "test:3")

	popPriority := Priority(-1)
	markCompletePriority := Priority(-2)

	tcs := []struct {
		Queue     string
		TaskGroup int
		TaskType  string
		Task      string
		Priority  Priority
	}{
		{"test", 1, "campaign", "task1", DefaultPriority},
		{"test", 1, "campaign", "task1", popPriority},
		{"test", 1, "campaign", "", popPriority},
		{"test", 1, "campaign", "task1", DefaultPriority},
		{"test", 1, "campaign", "task2", DefaultPriority},
		{"test", 2, "campaign", "task3", DefaultPriority},
		{"test", 2, "campaign", "task4", DefaultPriority},
		{"test", 1, "campaign", "task5", DefaultPriority},
		{"test", 2, "campaign", "task6", DefaultPriority},
		{"test", 1, "campaign", "task1", popPriority},
		{"test", 2, "campaign", "task3", popPriority},
		{"test", 1, "campaign", "task2", popPriority},
		{"test", 2, "campaign", "task4", popPriority},
		{"test", 2, "campaign", "", markCompletePriority},
		{"test", 2, "campaign", "task6", popPriority},
		{"test", 1, "campaign", "task5", popPriority},
		{"test", 1, "campaign", "", popPriority},
	}

	for i, tc := range tcs {
		if tc.Priority == popPriority {
			task, err := PopNextTask(rc, "test")

			if task == nil {
				if tc.Task != "" {
					assert.Fail(t, "%d: did not receive task, expected %s", i, tc.Task)
				}
				continue
			} else if tc.Task == "" && task != nil {
				assert.Fail(t, "%d: received task %s when expecting none", i, tc.Task)
				continue
			}

			assert.NoError(t, err)
			assert.Equal(t, task.OrgID, tc.TaskGroup, "%d: groups mismatch", i)
			assert.Equal(t, task.Type, tc.TaskType, "%d: types mismatch", i)

			var value string
			assert.NoError(t, json.Unmarshal(task.Task, &value), "%d: error unmarshalling", i)
			assert.Equal(t, value, tc.Task, "%d: task mismatch", i)
		} else if tc.Priority == markCompletePriority {
			assert.NoError(t, MarkTaskComplete(rc, tc.Queue, tc.TaskGroup))
		} else {
			assert.NoError(t, AddTask(rc, tc.Queue, tc.TaskType, tc.TaskGroup, tc.Task, tc.Priority))
		}
	}
}
