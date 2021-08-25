package models_test

import (
	"testing"

	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTopics(t *testing.T) {
	ctx, _, db, _ := testsuite.Get()

	oa, err := models.GetOrgAssetsWithRefresh(ctx, db, testdata.Org1.ID, models.RefreshTopics)
	require.NoError(t, err)

	topics, err := oa.Topics()
	require.NoError(t, err)

	assert.Equal(t, 1, len(topics))
	assert.Equal(t, testdata.DefaultTopic.UUID, topics[0].UUID())
	assert.Equal(t, "General", topics[0].Name())
}
