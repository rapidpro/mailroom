package models_test

import (
	"testing"

	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOptIns(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetData)

	polls := testdata.InsertOptIn(rt, testdata.Org1, "Polls")
	offers := testdata.InsertOptIn(rt, testdata.Org1, "Offers")

	oa, err := models.GetOrgAssetsWithRefresh(ctx, rt, testdata.Org1.ID, models.RefreshOptIns)
	require.NoError(t, err)

	optIns, err := oa.OptIns()
	require.NoError(t, err)

	assert.Equal(t, 2, len(optIns))
	assert.Equal(t, polls.UUID, optIns[0].UUID())
	assert.Equal(t, "Polls", optIns[0].Name())
	assert.Equal(t, offers.UUID, optIns[1].UUID())
	assert.Equal(t, "Offers", optIns[1].Name())
}
