package models_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/nyaruka/gocommon/i18n"
	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOrgs(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetAll)

	tz, _ := time.LoadLocation("America/Los_Angeles")

	rt.DB.MustExec("UPDATE channels_channel SET country = 'FR' WHERE id = $1;", testdata.FacebookChannel.ID)
	rt.DB.MustExec("UPDATE channels_channel SET country = 'US' WHERE id IN ($1,$2);", testdata.TwilioChannel.ID, testdata.VonageChannel.ID)

	rt.DB.MustExec(`UPDATE orgs_org SET flow_languages = '{"fra", "eng"}' WHERE id = $1`, testdata.Org1.ID)
	rt.DB.MustExec(`UPDATE orgs_org SET flow_languages = '{}' WHERE id = $1`, testdata.Org2.ID)
	rt.DB.MustExec(`UPDATE orgs_org SET date_format = 'M' WHERE id = $1`, testdata.Org2.ID)

	org, err := models.LoadOrg(ctx, rt.Config, rt.DB.DB, testdata.Org1.ID)
	assert.NoError(t, err)

	assert.Equal(t, models.OrgID(1), org.ID())
	assert.False(t, org.Suspended())
	assert.Equal(t, envs.DateFormatDayMonthYear, org.Environment().DateFormat())
	assert.Equal(t, envs.TimeFormatHourMinute, org.Environment().TimeFormat())
	assert.Equal(t, envs.RedactionPolicyNone, org.Environment().RedactionPolicy())
	assert.Equal(t, "US", string(org.Environment().DefaultCountry()))
	assert.Equal(t, tz, org.Environment().Timezone())
	assert.Equal(t, []i18n.Language{"fra", "eng"}, org.Environment().AllowedLanguages())
	assert.Equal(t, i18n.Language("fra"), org.Environment().DefaultLanguage())
	assert.Equal(t, i18n.Locale("fra-US"), org.Environment().DefaultLocale())

	org, err = models.LoadOrg(ctx, rt.Config, rt.DB.DB, testdata.Org2.ID)
	assert.NoError(t, err)
	assert.Equal(t, envs.DateFormatMonthDayYear, org.Environment().DateFormat())
	assert.Equal(t, []i18n.Language{}, org.Environment().AllowedLanguages())
	assert.Equal(t, i18n.NilLanguage, org.Environment().DefaultLanguage())
	assert.Equal(t, i18n.NilLocale, org.Environment().DefaultLocale())

	_, err = models.LoadOrg(ctx, rt.Config, rt.DB.DB, 99)
	assert.Error(t, err)
}

func TestStoreAttachment(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetStorage)

	image, err := os.Open("testdata/test.jpg")
	require.NoError(t, err)

	org, err := models.LoadOrg(ctx, rt.Config, rt.DB.DB, testdata.Org1.ID)
	assert.NoError(t, err)

	attachment, err := org.StoreAttachment(context.Background(), rt, "668383ba-387c-49bc-b164-1213ac0ea7aa.jpg", "image/jpeg", image)
	require.NoError(t, err)

	assert.Equal(t, utils.Attachment("image/jpeg:_test_attachments_storage/attachments/1/6683/83ba/668383ba-387c-49bc-b164-1213ac0ea7aa.jpg"), attachment)

	// err trying to read from same reader again
	_, err = org.StoreAttachment(context.Background(), rt, "668383ba-387c-49bc-b164-1213ac0ea7aa.jpg", "image/jpeg", image)
	assert.EqualError(t, err, "unable to read attachment content: read testdata/test.jpg: file already closed")
}
