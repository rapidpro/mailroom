package contact

import (
	"testing"
	"time"

	"github.com/nyaruka/gocommon/uuids"
	_ "github.com/nyaruka/mailroom/core/handlers"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/web"
)

func TestCreateContacts(t *testing.T) {
	testsuite.Reset()
	db := testsuite.DB()

	// detach Cathy's tel URN
	db.MustExec(`UPDATE contacts_contacturn SET contact_id = NULL WHERE contact_id = $1`, models.CathyID)

	db.MustExec(`ALTER SEQUENCE contacts_contact_id_seq RESTART WITH 30000`)

	web.RunWebTests(t, "testdata/create.json")
}

func TestModifyContacts(t *testing.T) {
	testsuite.Reset()
	db := testsuite.DB()

	// to be deterministic, update the creation date on cathy
	db.MustExec(`UPDATE contacts_contact SET created_on = $1 WHERE id = $2`, time.Date(2018, 7, 6, 12, 30, 0, 123456789, time.UTC), models.CathyID)

	// make our campaign group dynamic
	db.MustExec(`UPDATE contacts_contactgroup SET query = 'age > 18' WHERE id = $1`, models.DoctorsGroupID)

	// insert an event on our campaign that is based on created on
	db.MustExec(
		`INSERT INTO campaigns_campaignevent(is_active, created_on, modified_on, uuid, "offset", unit, event_type, delivery_hour, 
											 campaign_id, created_by_id, modified_by_id, flow_id, relative_to_id, start_mode)
									   VALUES(TRUE, NOW(), NOW(), $1, 1000, 'W', 'F', -1, $2, 1, 1, $3, $4, 'I')`,
		uuids.New(), models.DoctorRemindersCampaignID, models.FavoritesFlowID, models.CreatedOnFieldID)

	// for simpler tests we clear out cathy's fields and groups to start
	db.MustExec(`UPDATE contacts_contact SET fields = NULL WHERE id = $1`, models.CathyID)
	db.MustExec(`DELETE FROM contacts_contactgroup_contacts WHERE contact_id = $1`, models.CathyID)
	db.MustExec(`UPDATE contacts_contacturn SET contact_id = NULL WHERE contact_id = $1`, models.CathyID)

	// because we made changes to a group above, need to make sure we don't use stale org assets
	models.FlushCache()

	web.RunWebTests(t, "testdata/modify.json")

	models.FlushCache()
}

func TestResolveContacts(t *testing.T) {
	testsuite.Reset()
	db := testsuite.DB()

	// detach Cathy's tel URN
	db.MustExec(`UPDATE contacts_contacturn SET contact_id = NULL WHERE contact_id = $1`, models.CathyID)

	db.MustExec(`ALTER SEQUENCE contacts_contact_id_seq RESTART WITH 30000`)

	web.RunWebTests(t, "testdata/resolve.json")
}
