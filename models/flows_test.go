package models

import (
	"context"
	"testing"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows/definition"
	"github.com/stretchr/testify/assert"
)

func TestFlows(t *testing.T) {
	ctx := context.Background()
	db := Reset(t)

	tcs := []struct {
		FlowID   FlowID
		FlowUUID assets.FlowUUID
		Name     string
		Found    bool
	}{
		{FlowID(1), assets.FlowUUID("51e3c67d-8483-449c-abf7-25e50686f0db"), "Favorites", true},
	}

	for _, tc := range tcs {
		f, err := loadFlow(ctx, db, tc.FlowUUID)
		assert.NoError(t, err)

		flow := f.(*Flow)

		if tc.Found {
			assert.Equal(t, tc.Name, flow.Name())
			assert.Equal(t, tc.FlowID, flow.ID())
			assert.Equal(t, tc.FlowUUID, flow.UUID())

			_, err := definition.ReadFlow(flow.Definition())
			assert.NoError(t, err)
		}
	}
}
