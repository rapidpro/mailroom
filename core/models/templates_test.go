package models_test

import (
	"testing"

	"github.com/nyaruka/gocommon/i18n"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTemplates(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	oa, err := models.GetOrgAssetsWithRefresh(ctx, rt, testdata.Org1.ID, models.RefreshTemplates)
	require.NoError(t, err)

	templates, err := oa.Templates()
	require.NoError(t, err)

	assert.Equal(t, 2, len(templates))
	assert.Equal(t, "goodbye", templates[0].Name())
	assert.Equal(t, "revive_issue", templates[1].Name())

	assert.Equal(t, 1, len(templates[0].Translations()))
	tt := templates[0].Translations()[0]
	assert.Equal(t, i18n.Locale("fra"), tt.Locale())
	assert.Equal(t, "", tt.Namespace())
	assert.Equal(t, testdata.TwitterChannel.UUID, tt.Channel().UUID)
	assert.Equal(t, "Salut!", tt.Content())

	assert.Equal(t, 1, len(templates[1].Translations()))
	tt = templates[1].Translations()[0]
	assert.Equal(t, i18n.Locale("eng-US"), tt.Locale())
	assert.Equal(t, "2d40b45c_25cd_4965_9019_f05d0124c5fa", tt.Namespace())
	assert.Equal(t, testdata.TwitterChannel.UUID, tt.Channel().UUID)
	assert.Equal(t, "Hi {{1}}, are you still experiencing problems with {{2}}?", tt.Content())
}
