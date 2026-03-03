package tasks_test

import (
	"testing"

	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadTask(t *testing.T) {
	task, err := tasks.ReadTask("populate_group", []byte(`{
		"group_id": 23,
		"query": "gender = F"
	}`))
	require.NoError(t, err)

	typedTask := task.(*tasks.PopulateGroup)
	assert.Equal(t, models.GroupID(23), typedTask.GroupID)
	assert.Equal(t, "gender = F", typedTask.Query)
}
