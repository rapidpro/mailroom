package models

import (
	"context"
	"testing"

	"github.com/nyaruka/goflow/assets"
	"github.com/stretchr/testify/assert"
)

func TestFlows(t *testing.T) {
	ctx := context.Background()
	db := Reset(t)

	flowUUID := assets.FlowUUID("f6067e93-18dd-41a0-bd36-89d46fa4542b")
	f, err := loadFlow(ctx, db, flowUUID)
	assert.NoError(t, err)
	assert.NotNil(t, f)

	flow := f.(*Flow)
	assert.Equal(t, "Favorites", flow.Name())
	assert.Equal(t, FlowID(1), flow.ID())
	assert.Equal(t, flowUUID, flow.UUID())
}
