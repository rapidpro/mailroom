package models

import (
	"io/ioutil"
	"testing"
	"time"

	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/testsuite"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOrgs(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()

	tx, err := db.BeginTxx(ctx, nil)
	assert.NoError(t, err)
	defer tx.Rollback()

	tx.MustExec("UPDATE channels_channel SET country = 'FR' WHERE id = $1;", TwitterChannelID)
	tx.MustExec("UPDATE channels_channel SET country = 'US' WHERE id IN ($1,$2);", TwilioChannelID, NexmoChannelID)
	tx.MustExec(`INSERT INTO orgs_language(is_active, created_on, modified_on, name, iso_code, created_by_id, modified_by_id, org_id) 
									VALUES(TRUE, NOW(), NOW(), 'French', 'fra', 1, 1, 2);`)
	tx.MustExec(`INSERT INTO orgs_language(is_active, created_on, modified_on, name, iso_code, created_by_id, modified_by_id, org_id) 
									VALUES(TRUE, NOW(), NOW(), 'English', 'eng', 1, 1, 2);`)

	tx.MustExec("UPDATE orgs_org SET primary_language_id = 2 WHERE id = 2;")

	org, err := loadOrg(ctx, tx, 1)
	assert.NoError(t, err)

	assert.Equal(t, OrgID(1), org.ID())
	assert.False(t, org.Suspended())
	assert.True(t, org.UsesTopups())
	assert.Equal(t, envs.DateFormatDayMonthYear, org.DateFormat())
	assert.Equal(t, envs.TimeFormatHourMinute, org.TimeFormat())
	assert.Equal(t, envs.RedactionPolicyNone, org.RedactionPolicy())
	assert.Equal(t, 640, org.MaxValueLength())
	assert.Equal(t, string(envs.Country("US")), string(org.DefaultCountry()))
	tz, _ := time.LoadLocation("America/Los_Angeles")
	assert.Equal(t, tz, org.Timezone())
	assert.Equal(t, 0, len(org.AllowedLanguages()))
	assert.Equal(t, envs.Language(""), org.DefaultLanguage())
	assert.Equal(t, "", org.DefaultLocale().ToISO639_2())

	org, err = loadOrg(ctx, tx, 2)
	assert.NoError(t, err)
	assert.Equal(t, []envs.Language{"eng", "fra"}, org.AllowedLanguages())
	assert.Equal(t, envs.Language("eng"), org.DefaultLanguage())
	assert.Equal(t, "en", org.DefaultLocale().ToISO639_2())

	_, err = loadOrg(ctx, tx, 99)
	assert.Error(t, err)
}

func TestStoreAttachment(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()

	store := testsuite.Storage()
	defer testsuite.ResetStorage()

	image, err := ioutil.ReadFile("testdata/test.jpg")
	require.NoError(t, err)

	org, err := loadOrg(ctx, db, Org1)
	assert.NoError(t, err)

	attachment, err := org.StoreAttachment(store, "media", "668383ba-387c-49bc-b164-1213ac0ea7aa.jpg", image)
	require.NoError(t, err)

	assert.Equal(t, utils.Attachment("image/jpeg:_test_storage/media/1/6683/83ba/668383ba-387c-49bc-b164-1213ac0ea7aa.jpg"), attachment)
}
