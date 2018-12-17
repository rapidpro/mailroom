package models

import (
	"testing"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows/definition"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/stretchr/testify/assert"
)

func TestFlows(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()

	tcs := []struct {
		OrgID    OrgID
		FlowID   FlowID
		FlowUUID assets.FlowUUID
		Name     string
		Found    bool
	}{
		{OrgID(1), FlowID(1), assets.FlowUUID("51e3c67d-8483-449c-abf7-25e50686f0db"), "Favorites", true},
		{OrgID(2), FlowID(1), assets.FlowUUID("51e3c67d-8483-449c-abf7-25e50686f0db"), "", false},
	}

	for _, tc := range tcs {
		flow, err := loadFlowByUUID(ctx, db, tc.OrgID, tc.FlowUUID)
		assert.NoError(t, err)

		if tc.Found {
			assert.Equal(t, tc.Name, flow.Name())
			assert.Equal(t, tc.FlowID, flow.ID())
			assert.Equal(t, tc.FlowUUID, flow.UUID())

			_, err := definition.ReadFlow(flow.Definition())
			assert.NoError(t, err)
		} else {
			assert.Nil(t, flow)
		}

		flow, err = loadFlowByID(ctx, db, tc.OrgID, tc.FlowID)
		assert.NoError(t, err)

		if tc.Found {
			assert.Equal(t, tc.Name, flow.Name())
			assert.Equal(t, tc.FlowID, flow.ID())
			assert.Equal(t, tc.FlowUUID, flow.UUID())
		} else {
			assert.Nil(t, flow)
		}
	}
}
