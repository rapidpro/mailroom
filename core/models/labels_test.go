package models_test

import (
	"testing"

	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLabels(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	oa, err := models.GetOrgAssetsWithRefresh(ctx, rt, testdata.Org1.ID, models.RefreshLabels)
	require.NoError(t, err)

	labels, err := oa.Labels()
	require.NoError(t, err)

	tcs := []struct {
		ID   models.LabelID
		Name string
	}{
		{testdata.ReportingLabel.ID, "Reporting"},
		{testdata.TestingLabel.ID, "Testing"},
	}

	assert.Equal(t, 3, len(labels))
	for i, tc := range tcs {
		label := labels[i].(*models.Label)
		assert.Equal(t, tc.ID, label.ID())
		assert.Equal(t, tc.Name, label.Name())
	}
}
