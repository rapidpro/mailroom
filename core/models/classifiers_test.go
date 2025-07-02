package models_test

import (
	"testing"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClassifiers(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	oa, err := models.GetOrgAssetsWithRefresh(ctx, rt, testdata.Org1.ID, models.RefreshClassifiers)
	require.NoError(t, err)

	classifiers, err := oa.Classifiers()
	require.NoError(t, err)

	tcs := []struct {
		ID      models.ClassifierID
		UUID    assets.ClassifierUUID
		Name    string
		Intents []string
	}{
		{testdata.Luis.ID, testdata.Luis.UUID, "LUIS", []string{"book_flight", "book_car"}},
		{testdata.Wit.ID, testdata.Wit.UUID, "Wit.ai", []string{"register"}},
		{testdata.Bothub.ID, testdata.Bothub.UUID, "BotHub", []string{"intent"}},
	}

	assert.Equal(t, len(tcs), len(classifiers))
	for i, tc := range tcs {
		c := classifiers[i].(*models.Classifier)
		assert.Equal(t, tc.UUID, c.UUID())
		assert.Equal(t, tc.ID, c.ID())
		assert.Equal(t, tc.Name, c.Name())
		assert.Equal(t, tc.Intents, c.Intents())
	}

}
