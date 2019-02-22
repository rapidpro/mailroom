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
		Size      int
	}{
		{"test", 1, "campaign", "task1", DefaultPriority, 1},
		{"test", 1, "campaign", "task1", popPriority, 0},
		{"test", 1, "campaign", "", popPriority, 0},
		{"test", 1, "campaign", "task1", DefaultPriority, 1},
		{"test", 1, "campaign", "task2", DefaultPriority, 2},
		{"test", 2, "campaign", "task3", DefaultPriority, 3},
		{"test", 2, "campaign", "task4", DefaultPriority, 4},
		{"test", 1, "campaign", "task5", DefaultPriority, 5},
		{"test", 2, "campaign", "task6", DefaultPriority, 6},
		{"test", 1, "campaign", "task1", popPriority, 5},
		{"test", 2, "campaign", "task3", popPriority, 4},
		{"test", 1, "campaign", "task2", popPriority, 3},
		{"test", 2, "campaign", "task4", popPriority, 2},
		{"test", 2, "campaign", "", markCompletePriority, 2},
		{"test", 2, "campaign", "task6", popPriority, 1},
		{"test", 1, "campaign", "task5", popPriority, 0},
		{"test", 1, "campaign", "", popPriority, 0},
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

		size, err := Size(rc, tc.Queue)
		assert.NoError(t, err)
		assert.Equal(t, tc.Size, size, "%d: mismatch", i)
	}
}
