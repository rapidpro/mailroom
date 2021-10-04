package marker_test

import (
	"testing"

	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/utils/marker"

	"github.com/stretchr/testify/assert"
)

func TestMarker(t *testing.T) {
	_, _, _, rp := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetRedis)

	rc := rp.Get()
	defer rc.Close()

	tcs := []struct {
		Group  string
		TaskID string
		Action string
	}{
		{"1", "1", "remove"},
		{"2", "1", "remove"},
		{"1", "2", "remove"},
		{"1", "1", "absent"},
		{"1", "1", "add"},
		{"1", "1", "present"},
		{"2", "1", "absent"},
		{"1", "2", "absent"},
		{"1", "1", "remove"},
		{"1", "1", "absent"},
	}

	for i, tc := range tcs {
		if tc.Action == "absent" {
			present, err := marker.HasTask(rc, tc.Group, tc.TaskID)
			assert.NoError(t, err)
			assert.False(t, present, "%d: %s:%s should be absent", i, tc.Group, tc.TaskID)
		} else if tc.Action == "present" {
			present, err := marker.HasTask(rc, tc.Group, tc.TaskID)
			assert.NoError(t, err)
			assert.True(t, present, "%d: %s:%s should be present", i, tc.Group, tc.TaskID)
		} else if tc.Action == "add" {
			err := marker.AddTask(rc, tc.Group, tc.TaskID)
			assert.NoError(t, err)
		} else if tc.Action == "remove" {
			err := marker.RemoveTask(rc, tc.Group, tc.TaskID)
			assert.NoError(t, err)
		}
	}
}
