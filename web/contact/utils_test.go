package contact_test

import (
	"testing"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/web/contact"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateSpec(t *testing.T) {
	testsuite.Reset()
	db := testsuite.DB()
	ctx := testsuite.CTX()

	oa, err := models.GetOrgAssets(ctx, db, models.Org1)
	require.NoError(t, err)

	sa := oa.SessionAssets()
	env := envs.NewBuilder().Build()

	// empty spec is valid
	s := &contact.Spec{}
	c, err := s.Validate(env, sa)
	assert.NoError(t, err)
	assert.Equal(t, "", c.Name)
	assert.Equal(t, envs.NilLanguage, c.Language)
	assert.Equal(t, 0, len(c.URNs))
	assert.Equal(t, 0, len(c.Mods))

	// try to set invalid language
	s = &contact.Spec{Language: "xyzd"}
	_, err = s.Validate(env, sa)
	assert.EqualError(t, err, "invalid language: iso-639-3 codes must be 3 characters, got: xyzd")

	// try to set non-existent contact field
	s = &contact.Spec{Fields: map[string]string{"goats": "7"}}
	_, err = s.Validate(env, sa)
	assert.EqualError(t, err, "unknown contact field 'goats'")

	// try to add to non-existent group
	s = &contact.Spec{Groups: []assets.GroupUUID{"52f6c50e-f9a8-4f24-bb80-5c9f144ed27f"}}
	_, err = s.Validate(env, sa)
	assert.EqualError(t, err, "unknown contact group '52f6c50e-f9a8-4f24-bb80-5c9f144ed27f'")

	// try to add to dynamic group
	s = &contact.Spec{Groups: []assets.GroupUUID{"52f6c50e-f9a8-4f24-bb80-5c9f144ed27f"}}
	_, err = s.Validate(env, sa)
	assert.EqualError(t, err, "unknown contact group '52f6c50e-f9a8-4f24-bb80-5c9f144ed27f'")
}
