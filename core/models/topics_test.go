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
	ctx, rt := testsuite.Runtime()

	oa, err := models.GetOrgAssetsWithRefresh(ctx, rt, testdata.Org1.ID, models.RefreshTopics)
	require.NoError(t, err)

	topics, err := oa.Topics()
	require.NoError(t, err)

	assert.Equal(t, 3, len(topics))
	assert.Equal(t, testdata.DefaultTopic.UUID, topics[0].UUID())
	assert.Equal(t, "General", topics[0].Name())
	assert.Equal(t, testdata.SalesTopic.UUID, topics[1].UUID())
	assert.Equal(t, "Sales", topics[1].Name())
	assert.Equal(t, testdata.SupportTopic.UUID, topics[2].UUID())
	assert.Equal(t, "Support", topics[2].Name())

	assert.Equal(t, topics[1], oa.TopicByID(testdata.SalesTopic.ID))
	assert.Equal(t, topics[2], oa.TopicByUUID(testdata.SupportTopic.UUID))
}
