package contact_test

import (
	"testing"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/nyaruka/mailroom/web/contact"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSpecToCreation(t *testing.T) {
	ctx, rt, _, _ := testsuite.Get()

	oa, err := models.GetOrgAssets(ctx, rt, testdata.Org1.ID)
	require.NoError(t, err)

	sa := oa.SessionAssets()
	env := envs.NewBuilder().Build()

	// empty spec is valid
	s := &models.ContactSpec{}
	c, err := contact.SpecToCreation(s, env, sa)
	assert.NoError(t, err)
	assert.Equal(t, "", c.Name)
	assert.Equal(t, envs.NilLanguage, c.Language)
	assert.Equal(t, 0, len(c.URNs))
	assert.Equal(t, 0, len(c.Mods))

	// try to set invalid language
	lang := "xyzd"
	s = &models.ContactSpec{Language: &lang}
	_, err = contact.SpecToCreation(s, env, sa)
	assert.EqualError(t, err, "invalid language: iso-639-3 codes must be 3 characters, got: xyzd")

	// try to set non-existent contact field
	s = &models.ContactSpec{Fields: map[string]string{"goats": "7"}}
	_, err = contact.SpecToCreation(s, env, sa)
	assert.EqualError(t, err, "unknown contact field 'goats'")

	// try to add to non-existent group
	s = &models.ContactSpec{Groups: []assets.GroupUUID{"52f6c50e-f9a8-4f24-bb80-5c9f144ed27f"}}
	_, err = contact.SpecToCreation(s, env, sa)
	assert.EqualError(t, err, "unknown contact group '52f6c50e-f9a8-4f24-bb80-5c9f144ed27f'")

	// try to add to dynamic group
	s = &models.ContactSpec{Groups: []assets.GroupUUID{"52f6c50e-f9a8-4f24-bb80-5c9f144ed27f"}}
	_, err = contact.SpecToCreation(s, env, sa)
	assert.EqualError(t, err, "unknown contact group '52f6c50e-f9a8-4f24-bb80-5c9f144ed27f'")
}
