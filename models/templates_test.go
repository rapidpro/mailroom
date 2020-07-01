package models

import (
	"testing"

	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/mailroom/testsuite"

	"github.com/stretchr/testify/assert"
)

func TestTemplates(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()

	templates, err := loadTemplates(ctx, db, 1)
	assert.NoError(t, err)

	assert.Equal(t, 2, len(templates))
	assert.Equal(t, "goodbye", templates[0].Name())
	assert.Equal(t, "revive_issue", templates[1].Name())

	assert.Equal(t, 1, len(templates[0].Translations()))
	tt := templates[0].Translations()[0]
	assert.Equal(t, envs.Language("fra"), tt.Language())
	assert.Equal(t, envs.NilCountry, tt.Country())
	assert.Equal(t, TwitterChannelUUID, tt.Channel().UUID)
	assert.Equal(t, "Salut!", tt.Content())

	assert.Equal(t, 1, len(templates[1].Translations()))
	tt = templates[1].Translations()[0]
	assert.Equal(t, envs.Language("eng"), tt.Language())
	assert.Equal(t, envs.Country("US"), tt.Country())
	assert.Equal(t, TwitterChannelUUID, tt.Channel().UUID)
	assert.Equal(t, "Hi {{1}}, are you still experiencing problems with {{2}}?", tt.Content())
}
