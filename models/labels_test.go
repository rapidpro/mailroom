package models

import (
	"context"
	"testing"

	"github.com/nyaruka/goflow/flows"
	"github.com/stretchr/testify/assert"
)

func TestLabels(t *testing.T) {
	ctx := context.Background()
	db := Reset(t)

	labels, err := loadLabels(ctx, db, 1)
	assert.NoError(t, err)

	tcs := []struct {
		ID   flows.LabelID
		Name string
	}{
		{flows.LabelID(9), "Building"},
		{flows.LabelID(8), "Driving"},
	}

	assert.Equal(t, 10, len(labels))
	for i, tc := range tcs {
		assert.Equal(t, tc.ID, labels[i].ID())
		assert.Equal(t, tc.Name, labels[i].Name())
	}
}
