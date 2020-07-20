package models_test

import (
	"encoding/json"
	"testing"

	"github.com/nyaruka/mailroom/models"
	"github.com/stretchr/testify/assert"
)

func TestStarts(t *testing.T) {
	startJSON := []byte(`{
		"start_id": 123,
		"start_type": "M",
		"org_id": 12,
		"created_by": "rowan@nyaruka.com",
		"flow_id": 234,
		"flow_type": "M",
		"contact_ids": [4567, 5678],
		"group_ids": [6789],
		"query": null,
		"restart_participants": true,
		"include_active": true,
		"extra": {"foo": "bar"}
	}`)

	start := &models.FlowStart{}
	err := json.Unmarshal(startJSON, start)

	assert.NoError(t, err)
	assert.Equal(t, models.StartID(123), start.ID())
	assert.Equal(t, models.OrgID(12), start.OrgID())
	assert.Equal(t, models.FlowID(234), start.FlowID())
	assert.Equal(t, models.MessagingFlow, start.FlowType())
	assert.Equal(t, "", start.Query())
	assert.Equal(t, models.DoRestartParticipants, start.RestartParticipants())
	assert.Equal(t, models.DoIncludeActive, start.IncludeActive())
	assert.Equal(t, json.RawMessage(`{"foo": "bar"}`), start.Extra())

	batch := start.CreateBatch([]models.ContactID{4567, 5678}, false, 3)
	assert.Equal(t, models.StartID(123), batch.StartID())
	assert.Equal(t, models.StartTypeManual, batch.StartType())
	assert.Equal(t, models.FlowID(234), batch.FlowID())
	assert.Equal(t, []models.ContactID{4567, 5678}, batch.ContactIDs())
	assert.Equal(t, models.DoRestartParticipants, batch.RestartParticipants())
	assert.Equal(t, models.DoIncludeActive, batch.IncludeActive())
	assert.Equal(t, "rowan@nyaruka.com", batch.CreatedBy())
	assert.False(t, batch.IsLast())
	assert.Equal(t, 3, batch.TotalContacts())
}
