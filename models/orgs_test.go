package models

import (
	"testing"
	"time"

	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/stretchr/testify/assert"
)

func TestOrgs(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()

	tx, err := db.BeginTxx(ctx, nil)
	assert.NoError(t, err)
	defer tx.Rollback()

	tx.MustExec("UPDATE orgs_org SET language = 'eng' WHERE id = 2;")
	tx.MustExec(`INSERT INTO orgs_language(is_active, created_on, modified_on, name, iso_code, created_by_id, modified_by_id, org_id) 
				                    VALUES(TRUE, NOW(), NOW(), 'French', 'fra', 1, 1, 2);`)

	org, err := loadOrg(ctx, tx, 1)
	assert.NoError(t, err)

	assert.Equal(t, OrgID(1), org.ID())
	assert.Equal(t, utils.DateFormatDayMonthYear, org.DateFormat())
	assert.Equal(t, utils.TimeFormatHourMinute, org.TimeFormat())
	assert.Equal(t, utils.RedactionPolicyNone, org.RedactionPolicy())
	tz, _ := time.LoadLocation("Europe/Copenhagen")
	assert.Equal(t, tz, org.Timezone())
	assert.Equal(t, 0, len(org.AllowedLanguages()))

	org, err = loadOrg(ctx, tx, 2)
	assert.NoError(t, err)
	assert.Equal(t, []utils.Language{"eng", "fra"}, org.AllowedLanguages())

	_, err = loadOrg(ctx, tx, 99)
	assert.Error(t, err)
}
