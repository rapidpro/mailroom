package models_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/nyaruka/mailroom/utils/test"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestContacts(t *testing.T) {
	testsuite.Reset()
	ctx := testsuite.CTX()
	db := testsuite.DB()

	org, err := models.GetOrgAssets(ctx, db, 1)
	assert.NoError(t, err)

	testdata.InsertContactURN(t, db, models.Org1, models.BobID, urns.URN("whatsapp:250788373373"), 999)

	db.MustExec(`DELETE FROM contacts_contacturn WHERE contact_id = $1`, models.GeorgeID)
	db.MustExec(`DELETE FROM contacts_contactgroup_contacts WHERE contact_id = $1`, models.GeorgeID)
	db.MustExec(`UPDATE contacts_contact SET is_active = FALSE WHERE id = $1`, models.AlexandriaID)

	modelContacts, err := models.LoadContacts(ctx, db, org, []models.ContactID{models.CathyID, models.GeorgeID, models.BobID, models.AlexandriaID})
	assert.NoError(t, err)
	assert.Equal(t, 3, len(modelContacts))

	// convert to goflow contacts
	contacts := make([]*flows.Contact, len(modelContacts))
	for i := range modelContacts {
		contacts[i], err = modelContacts[i].FlowContact(org)
		assert.NoError(t, err)
	}

	if len(contacts) == 3 {
		assert.Equal(t, "Cathy", contacts[0].Name())
		assert.Equal(t, len(contacts[0].URNs()), 1)
		assert.Equal(t, contacts[0].URNs()[0].String(), "tel:+16055741111?id=10000&priority=1000")
		assert.Equal(t, 1, contacts[0].Groups().Count())

		assert.Equal(t, "Yobe", contacts[0].Fields()["state"].QueryValue())
		assert.Equal(t, "Dokshi", contacts[0].Fields()["ward"].QueryValue())
		assert.Equal(t, "F", contacts[0].Fields()["gender"].QueryValue())
		assert.Equal(t, (*flows.FieldValue)(nil), contacts[0].Fields()["age"])

		assert.Equal(t, "Bob", contacts[1].Name())
		assert.NotNil(t, contacts[1].Fields()["joined"].QueryValue())
		assert.Equal(t, 2, len(contacts[1].URNs()))
		assert.Equal(t, contacts[1].URNs()[0].String(), "tel:+16055742222?id=10001&priority=1000")
		assert.Equal(t, contacts[1].URNs()[1].String(), "whatsapp:250788373373?id=20121&priority=999")
		assert.Equal(t, 0, contacts[1].Groups().Count())

		assert.Equal(t, "George", contacts[2].Name())
		assert.Equal(t, decimal.RequireFromString("30"), contacts[2].Fields()["age"].QueryValue())
		assert.Equal(t, 0, len(contacts[2].URNs()))
		assert.Equal(t, 0, contacts[2].Groups().Count())
	}

	// change bob to have a preferred URN and channel of our telephone
	channel := org.ChannelByID(models.TwilioChannelID)
	err = modelContacts[1].UpdatePreferredURN(ctx, db, org, models.BobURNID, channel)
	assert.NoError(t, err)

	bob, err := modelContacts[1].FlowContact(org)
	assert.NoError(t, err)
	assert.Equal(t, "tel:+16055742222?channel=74729f45-7f29-4868-9dc4-90e491e3c7d8&id=10001&priority=1000", bob.URNs()[0].String())
	assert.Equal(t, "whatsapp:250788373373?id=20121&priority=999", bob.URNs()[1].String())

	// add another tel urn to bob
	testdata.InsertContactURN(t, db, models.Org1, models.BobID, urns.URN("tel:+250788373373"), 10)

	// reload the contact
	modelContacts, err = models.LoadContacts(ctx, db, org, []models.ContactID{models.BobID})
	assert.NoError(t, err)

	// set our preferred channel again
	err = modelContacts[0].UpdatePreferredURN(ctx, db, org, models.URNID(20122), channel)
	assert.NoError(t, err)

	bob, err = modelContacts[0].FlowContact(org)
	assert.NoError(t, err)
	assert.Equal(t, "tel:+250788373373?channel=74729f45-7f29-4868-9dc4-90e491e3c7d8&id=20122&priority=1000", bob.URNs()[0].String())
	assert.Equal(t, "tel:+16055742222?channel=74729f45-7f29-4868-9dc4-90e491e3c7d8&id=10001&priority=999", bob.URNs()[1].String())
	assert.Equal(t, "whatsapp:250788373373?id=20121&priority=998", bob.URNs()[2].String())

	// no op this time
	err = modelContacts[0].UpdatePreferredURN(ctx, db, org, models.URNID(20122), channel)
	assert.NoError(t, err)

	bob, err = modelContacts[0].FlowContact(org)
	assert.NoError(t, err)
	assert.Equal(t, "tel:+250788373373?channel=74729f45-7f29-4868-9dc4-90e491e3c7d8&id=20122&priority=1000", bob.URNs()[0].String())
	assert.Equal(t, "tel:+16055742222?channel=74729f45-7f29-4868-9dc4-90e491e3c7d8&id=10001&priority=999", bob.URNs()[1].String())
	assert.Equal(t, "whatsapp:250788373373?id=20121&priority=998", bob.URNs()[2].String())

	// calling with no channel is a noop on the channel
	err = modelContacts[0].UpdatePreferredURN(ctx, db, org, models.URNID(20122), nil)
	assert.NoError(t, err)

	bob, err = modelContacts[0].FlowContact(org)
	assert.NoError(t, err)
	assert.Equal(t, "tel:+250788373373?channel=74729f45-7f29-4868-9dc4-90e491e3c7d8&id=20122&priority=1000", bob.URNs()[0].String())
	assert.Equal(t, "tel:+16055742222?channel=74729f45-7f29-4868-9dc4-90e491e3c7d8&id=10001&priority=999", bob.URNs()[1].String())
	assert.Equal(t, "whatsapp:250788373373?id=20121&priority=998", bob.URNs()[2].String())
}

func TestCreateContact(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()
	testsuite.Reset()
	models.FlushCache()

	testdata.InsertContactGroup(t, db, models.Org1, "d636c966-79c1-4417-9f1c-82ad629773a2", "Kinyarwanda", "language = kin")

	// add an orphaned URN
	testdata.InsertContactURN(t, db, models.Org1, models.NilContactID, urns.URN("telegram:200002"), 100)

	oa, err := models.GetOrgAssets(ctx, db, models.Org1)
	require.NoError(t, err)

	contact, flowContact, err := models.CreateContact(ctx, db, oa, models.UserID(1), "Rich", envs.Language(`kin`), []urns.URN{urns.URN("telegram:200001"), urns.URN("telegram:200002")})
	require.NoError(t, err)

	assert.Equal(t, "Rich", contact.Name())
	assert.Equal(t, envs.Language(`kin`), contact.Language())
	assert.Equal(t, []urns.URN{"telegram:200001?id=20122&priority=1000", "telegram:200002?id=20121&priority=999"}, contact.URNs())

	assert.Equal(t, "Rich", flowContact.Name())
	assert.Equal(t, envs.Language(`kin`), flowContact.Language())
	assert.Equal(t, []urns.URN{"telegram:200001?id=20122&priority=1000", "telegram:200002?id=20121&priority=999"}, flowContact.URNs().RawURNs())
	assert.Len(t, flowContact.Groups().All(), 1)
	assert.Equal(t, assets.GroupUUID("d636c966-79c1-4417-9f1c-82ad629773a2"), flowContact.Groups().All()[0].UUID())

	_, _, err = models.CreateContact(ctx, db, oa, models.UserID(1), "Rich", envs.Language(`kin`), []urns.URN{urns.URN("telegram:200001")})
	assert.EqualError(t, err, "URNs in use by other contacts")
}

func TestCreateContactRace(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()

	oa, err := models.GetOrgAssets(ctx, db, models.Org1)
	assert.NoError(t, err)

	mdb := testsuite.NewMockDB(db, func(funcName string, call int) error {
		// Make beginning a transaction take a while to create race condition. All threads will fetch
		// URN owners and decide nobody owns the URN, so all threads will try to create a new contact.
		if funcName == "BeginTxx" {
			time.Sleep(100 * time.Millisecond)
		}
		return nil
	})

	var contacts [2]*models.Contact
	var errs [2]error

	test.RunConcurrently(2, func(i int) {
		contacts[i], _, errs[i] = models.CreateContact(ctx, mdb, oa, models.UserID(1), "", envs.NilLanguage, []urns.URN{urns.URN("telegram:100007")})
	})

	// one should return a contact, the other should error
	require.True(t, (errs[0] != nil && errs[1] == nil) || (errs[0] == nil && errs[1] != nil))
	require.True(t, (contacts[0] != nil && contacts[1] == nil) || (contacts[0] == nil && contacts[1] != nil))
}

func TestGetOrCreateContact(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()
	testsuite.Reset()

	testdata.InsertContactGroup(t, db, models.Org1, "d636c966-79c1-4417-9f1c-82ad629773a2", "Telegrammer", `telegram = 100001`)

	// add some orphaned URNs
	testdata.InsertContactURN(t, db, models.Org1, models.NilContactID, urns.URN("telegram:200001"), 100)
	testdata.InsertContactURN(t, db, models.Org1, models.NilContactID, urns.URN("telegram:200002"), 100)

	var maxContactID models.ContactID
	db.Get(&maxContactID, `SELECT max(id) FROM contacts_contact`)
	newContact := func() models.ContactID { maxContactID++; return maxContactID }
	prevContact := func() models.ContactID { return maxContactID }

	models.FlushCache()
	oa, err := models.GetOrgAssets(ctx, db, models.Org1)
	require.NoError(t, err)

	tcs := []struct {
		OrgID       models.OrgID
		URNs        []urns.URN
		ContactID   models.ContactID
		Created     bool
		ContactURNs []urns.URN
		ChannelID   models.ChannelID
		GroupsUUIDs []assets.GroupUUID
	}{
		{
			models.Org1,
			[]urns.URN{models.CathyURN},
			models.CathyID,
			false,
			[]urns.URN{"tel:+16055741111?id=10000&priority=1000"},
			models.NilChannelID,
			[]assets.GroupUUID{models.DoctorsGroupUUID},
		},
		{
			models.Org1,
			[]urns.URN{urns.URN(models.CathyURN.String() + "?foo=bar")},
			models.CathyID, // only URN identity is considered
			false,
			[]urns.URN{"tel:+16055741111?id=10000&priority=1000"},
			models.NilChannelID,
			[]assets.GroupUUID{models.DoctorsGroupUUID},
		},
		{
			models.Org1,
			[]urns.URN{urns.URN("telegram:100001")},
			newContact(), // creates new contact
			true,
			[]urns.URN{"telegram:100001?channel=74729f45-7f29-4868-9dc4-90e491e3c7d8&id=20123&priority=1000"},
			models.TwilioChannelID,
			[]assets.GroupUUID{"d636c966-79c1-4417-9f1c-82ad629773a2"},
		},
		{
			models.Org1,
			[]urns.URN{urns.URN("telegram:100001")},
			prevContact(), // returns the same created contact
			false,
			[]urns.URN{"telegram:100001?channel=74729f45-7f29-4868-9dc4-90e491e3c7d8&id=20123&priority=1000"},
			models.NilChannelID,
			[]assets.GroupUUID{"d636c966-79c1-4417-9f1c-82ad629773a2"},
		},
		{
			models.Org1,
			[]urns.URN{urns.URN("telegram:100001"), urns.URN("telegram:100002")},
			prevContact(), // same again as other URNs don't exist
			false,
			[]urns.URN{"telegram:100001?channel=74729f45-7f29-4868-9dc4-90e491e3c7d8&id=20123&priority=1000"},
			models.NilChannelID,
			[]assets.GroupUUID{"d636c966-79c1-4417-9f1c-82ad629773a2"},
		},
		{
			models.Org1,
			[]urns.URN{urns.URN("telegram:100002"), urns.URN("telegram:100001")},
			prevContact(), // same again as other URNs don't exist
			false,
			[]urns.URN{"telegram:100001?channel=74729f45-7f29-4868-9dc4-90e491e3c7d8&id=20123&priority=1000"},
			models.NilChannelID,
			[]assets.GroupUUID{"d636c966-79c1-4417-9f1c-82ad629773a2"},
		},
		{
			models.Org1,
			[]urns.URN{urns.URN("telegram:200001"), urns.URN("telegram:100001")},
			prevContact(), // same again as other URNs are orphaned
			false,
			[]urns.URN{"telegram:100001?channel=74729f45-7f29-4868-9dc4-90e491e3c7d8&id=20123&priority=1000"},
			models.NilChannelID,
			[]assets.GroupUUID{"d636c966-79c1-4417-9f1c-82ad629773a2"},
		},
		{
			models.Org1,
			[]urns.URN{urns.URN("telegram:100003"), urns.URN("telegram:100004")}, // 2 new URNs
			newContact(),
			true,
			[]urns.URN{"telegram:100003?id=20124&priority=1000", "telegram:100004?id=20125&priority=999"},
			models.NilChannelID,
			[]assets.GroupUUID{},
		},
		{
			models.Org1,
			[]urns.URN{urns.URN("telegram:100005"), urns.URN("telegram:200002")}, // 1 new, 1 orphaned
			newContact(),
			true,
			[]urns.URN{"telegram:100005?id=20126&priority=1000", "telegram:200002?id=20122&priority=999"},
			models.NilChannelID,
			[]assets.GroupUUID{},
		},
	}

	for i, tc := range tcs {
		contact, flowContact, created, err := models.GetOrCreateContact(ctx, db, oa, tc.URNs, tc.ChannelID)
		assert.NoError(t, err, "%d: error creating contact", i)

		assert.Equal(t, tc.ContactID, contact.ID(), "%d: contact id mismatch", i)
		assert.Equal(t, tc.ContactURNs, flowContact.URNs().RawURNs(), "%d: URNs mismatch", i)
		assert.Equal(t, tc.Created, created, "%d: created flag mismatch", i)

		groupUUIDs := make([]assets.GroupUUID, len(flowContact.Groups().All()))
		for i, g := range flowContact.Groups().All() {
			groupUUIDs[i] = g.UUID()
		}

		assert.Equal(t, tc.GroupsUUIDs, groupUUIDs, "%d: groups mismatch", i)
	}
}

func TestGetOrCreateContactRace(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()

	oa, err := models.GetOrgAssets(ctx, db, models.Org1)
	assert.NoError(t, err)

	mdb := testsuite.NewMockDB(db, func(funcName string, call int) error {
		// Make beginning a transaction take a while to create race condition. All threads will fetch
		// URN owners and decide nobody owns the URN, so all threads will try to create a new contact.
		if funcName == "BeginTxx" {
			time.Sleep(100 * time.Millisecond)
		}
		return nil
	})

	var contacts [2]*models.Contact
	var errs [2]error

	test.RunConcurrently(2, func(i int) {
		contacts[i], _, _, errs[i] = models.GetOrCreateContact(ctx, mdb, oa, []urns.URN{urns.URN("telegram:100007")}, models.NilChannelID)
	})

	require.NoError(t, errs[0])
	require.NoError(t, errs[1])
	assert.Equal(t, contacts[0].ID(), contacts[1].ID())
}

func TestGetOrCreateContactIDsFromURNs(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()
	testsuite.Reset()

	// add an orphaned URN
	testdata.InsertContactURN(t, db, models.Org1, models.NilContactID, urns.URN("telegram:200001"), 100)

	var maxContactID models.ContactID
	db.Get(&maxContactID, `SELECT max(id) FROM contacts_contact`)
	newContact := func() models.ContactID { maxContactID++; return maxContactID }
	prevContact := func() models.ContactID { return maxContactID }

	models.FlushCache()
	org, err := models.GetOrgAssets(ctx, db, models.Org1)
	assert.NoError(t, err)

	tcs := []struct {
		OrgID      models.OrgID
		URNs       []urns.URN
		ContactIDs map[urns.URN]models.ContactID
	}{
		{
			models.Org1,
			[]urns.URN{models.CathyURN},
			map[urns.URN]models.ContactID{models.CathyURN: models.CathyID},
		},
		{
			models.Org1,
			[]urns.URN{urns.URN(models.CathyURN.String() + "?foo=bar")},
			map[urns.URN]models.ContactID{urns.URN(models.CathyURN.String() + "?foo=bar"): models.CathyID},
		},
		{
			models.Org1,
			[]urns.URN{models.CathyURN, urns.URN("telegram:100001")},
			map[urns.URN]models.ContactID{
				models.CathyURN:             models.CathyID,
				urns.URN("telegram:100001"): newContact(),
			},
		},
		{
			models.Org1,
			[]urns.URN{urns.URN("telegram:100001")},
			map[urns.URN]models.ContactID{urns.URN("telegram:100001"): prevContact()},
		},
		{
			models.Org1,
			[]urns.URN{urns.URN("telegram:200001")},
			map[urns.URN]models.ContactID{urns.URN("telegram:200001"): newContact()}, // new contact assigned orphaned URN
		},
	}

	for i, tc := range tcs {
		ids, err := models.GetOrCreateContactIDsFromURNs(ctx, db, org, tc.URNs)
		assert.NoError(t, err, "%d: error getting contact ids", i)
		assert.Equal(t, tc.ContactIDs, ids, "%d: mismatch in contact ids", i)
	}
}

func TestGetOrCreateContactIDsFromURNsRace(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()

	models.FlushCache()
	oa, err := models.GetOrgAssets(ctx, db, models.Org1)
	assert.NoError(t, err)

	mdb := testsuite.NewMockDB(db, func(funcName string, call int) error {
		// Make beginning a transaction take a while to create race condition. All threads will fetch
		// URN owners and decide nobody owns the URN, so all threads will try to create a new contact.
		if funcName == "BeginTxx" {
			time.Sleep(100 * time.Millisecond)
		}
		return nil
	})

	var contacts [2]models.ContactID
	var errs [2]error

	test.RunConcurrently(2, func(i int) {
		var cmap map[urns.URN]models.ContactID
		cmap, errs[i] = models.GetOrCreateContactIDsFromURNs(ctx, mdb, oa, []urns.URN{urns.URN("telegram:100007")})
		contacts[i] = cmap[urns.URN("telegram:100007")]
	})

	require.NoError(t, errs[0])
	require.NoError(t, errs[1])
	assert.Equal(t, contacts[0], contacts[1])
}

func TestGetContactIDsFromReferences(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()

	ids, err := models.GetContactIDsFromReferences(ctx, db, models.Org1, []*flows.ContactReference{
		flows.NewContactReference(models.CathyUUID, "Cathy"),
		flows.NewContactReference(models.BobUUID, "Bob"),
	})
	require.NoError(t, err)
	assert.ElementsMatch(t, []models.ContactID{models.CathyID, models.BobID}, ids)
}

func TestStopContact(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()

	// stop kathy
	err := models.StopContact(ctx, db, models.Org1, models.CathyID)
	assert.NoError(t, err)

	// verify she's only in the stopped group
	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM contacts_contactgroup_contacts WHERE contact_id = $1`, []interface{}{models.CathyID}, 1)

	// verify she's stopped
	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM contacts_contact WHERE id = $1 AND status = 'S' AND is_active = TRUE`, []interface{}{models.CathyID}, 1)
}

func TestUpdateContactLastSeenAndModifiedOn(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()
	testsuite.Reset()

	oa, err := models.GetOrgAssets(ctx, db, models.Org1)
	require.NoError(t, err)

	t0 := time.Now()

	err = models.UpdateContactModifiedOn(ctx, db, []models.ContactID{models.CathyID})
	assert.NoError(t, err)

	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM contacts_contact WHERE modified_on > $1 AND last_seen_on IS NULL`, []interface{}{t0}, 1)

	t1 := time.Now().Truncate(time.Millisecond)
	time.Sleep(time.Millisecond * 5)

	err = models.UpdateContactLastSeenOn(ctx, db, models.CathyID, t1)
	assert.NoError(t, err)

	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM contacts_contact WHERE modified_on > $1 AND last_seen_on = $1`, []interface{}{t1}, 1)

	cathy, err := models.LoadContact(ctx, db, oa, models.CathyID)
	require.NoError(t, err)
	assert.NotNil(t, cathy.LastSeenOn())
	assert.True(t, t1.Equal(*cathy.LastSeenOn()))
	assert.True(t, cathy.ModifiedOn().After(t1))

	t2 := time.Now().Truncate(time.Millisecond)
	time.Sleep(time.Millisecond * 5)

	// can update directly from the contact object
	err = cathy.UpdateLastSeenOn(ctx, db, t2)
	require.NoError(t, err)
	assert.True(t, t2.Equal(*cathy.LastSeenOn()))

	// and that also updates the database
	cathy, err = models.LoadContact(ctx, db, oa, models.CathyID)
	require.NoError(t, err)
	assert.True(t, t2.Equal(*cathy.LastSeenOn()))
	assert.True(t, cathy.ModifiedOn().After(t2))
}

func TestUpdateContactModifiedBy(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()
	testsuite.Reset()

	err := models.UpdateContactModifiedBy(ctx, db, []models.ContactID{}, models.UserID(0))
	assert.NoError(t, err)

	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM contacts_contact WHERE id = $1 AND modified_by_id = $2`, []interface{}{models.CathyID, models.UserID(0)}, 0)

	err = models.UpdateContactModifiedBy(ctx, db, []models.ContactID{models.CathyID}, models.UserID(0))
	assert.NoError(t, err)

	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM contacts_contact WHERE id = $1 AND modified_by_id = $2`, []interface{}{models.CathyID, models.UserID(0)}, 0)

	err = models.UpdateContactModifiedBy(ctx, db, []models.ContactID{models.CathyID}, models.UserID(1))
	assert.NoError(t, err)

	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM contacts_contact WHERE id = $1 AND modified_by_id = $2`, []interface{}{models.CathyID, models.UserID(1)}, 1)
}

func TestUpdateContactStatus(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()
	testsuite.Reset()

	err := models.UpdateContactStatus(ctx, db, []*models.ContactStatusChange{})
	assert.NoError(t, err)

	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM contacts_contact WHERE id = $1 AND status = 'B'`, []interface{}{models.CathyID}, 0)
	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM contacts_contact WHERE id = $1 AND status = 'S'`, []interface{}{models.CathyID}, 0)

	changes := make([]*models.ContactStatusChange, 0, 1)
	changes = append(changes, &models.ContactStatusChange{models.CathyID, flows.ContactStatusBlocked})

	err = models.UpdateContactStatus(ctx, db, changes)

	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM contacts_contact WHERE id = $1 AND status = 'B'`, []interface{}{models.CathyID}, 1)
	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM contacts_contact WHERE id = $1 AND status = 'S'`, []interface{}{models.CathyID}, 0)

	changes = make([]*models.ContactStatusChange, 0, 1)
	changes = append(changes, &models.ContactStatusChange{models.CathyID, flows.ContactStatusStopped})

	err = models.UpdateContactStatus(ctx, db, changes)

	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM contacts_contact WHERE id = $1 AND status = 'B'`, []interface{}{models.CathyID}, 0)
	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM contacts_contact WHERE id = $1 AND status = 'S'`, []interface{}{models.CathyID}, 1)

}

func TestUpdateContactURNs(t *testing.T) {
	testsuite.Reset()
	ctx := testsuite.CTX()
	db := testsuite.DB()
	testsuite.Reset()

	oa, err := models.GetOrgAssets(ctx, db, models.Org1)
	assert.NoError(t, err)

	numInitialURNs := 0
	db.Get(&numInitialURNs, `SELECT count(*) FROM contacts_contacturn`)

	assertContactURNs := func(contactID models.ContactID, expected []string) {
		var actual []string
		err = db.Select(&actual, `SELECT identity FROM contacts_contacturn WHERE contact_id = $1 ORDER BY priority DESC`, contactID)
		assert.NoError(t, err)
		assert.Equal(t, expected, actual, "URNs mismatch for contact %d", contactID)
	}

	assertContactURNs(models.CathyID, []string{"tel:+16055741111"})
	assertContactURNs(models.BobID, []string{"tel:+16055742222"})
	assertContactURNs(models.GeorgeID, []string{"tel:+16055743333"})

	cathyURN := urns.URN(fmt.Sprintf("tel:+16055741111?id=%d", models.CathyURNID))
	bobURN := urns.URN(fmt.Sprintf("tel:+16055742222?id=%d", models.BobURNID))

	// give Cathy a new higher priority URN
	err = models.UpdateContactURNs(ctx, db, oa, []*models.ContactURNsChanged{{models.CathyID, models.Org1, []urns.URN{"tel:+16055700001", cathyURN}}})
	assert.NoError(t, err)

	assertContactURNs(models.CathyID, []string{"tel:+16055700001", "tel:+16055741111"})

	// give Bob a new lower priority URN
	err = models.UpdateContactURNs(ctx, db, oa, []*models.ContactURNsChanged{{models.BobID, models.Org1, []urns.URN{bobURN, "tel:+16055700002"}}})
	assert.NoError(t, err)

	assertContactURNs(models.BobID, []string{"tel:+16055742222", "tel:+16055700002"})
	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM contacts_contacturn WHERE contact_id IS NULL`, nil, 0) // shouldn't be any orphan URNs
	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM contacts_contacturn`, nil, numInitialURNs+2)           // but 2 new URNs

	// remove a URN from Cathy
	err = models.UpdateContactURNs(ctx, db, oa, []*models.ContactURNsChanged{{models.CathyID, models.Org1, []urns.URN{"tel:+16055700001"}}})
	assert.NoError(t, err)

	assertContactURNs(models.CathyID, []string{"tel:+16055700001"})
	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM contacts_contacturn WHERE contact_id IS NULL`, nil, 1) // now orphaned

	// steal a URN from Bob
	err = models.UpdateContactURNs(ctx, db, oa, []*models.ContactURNsChanged{{models.CathyID, models.Org1, []urns.URN{"tel:+16055700001", "tel:+16055700002"}}})
	assert.NoError(t, err)

	assertContactURNs(models.CathyID, []string{"tel:+16055700001", "tel:+16055700002"})
	assertContactURNs(models.BobID, []string{"tel:+16055742222"})

	// steal the URN back from Cathy whilst simulataneously adding new URN to Cathy and not-changing anything for George
	err = models.UpdateContactURNs(ctx, db, oa, []*models.ContactURNsChanged{
		{models.BobID, models.Org1, []urns.URN{"tel:+16055742222", "tel:+16055700002"}},
		{models.CathyID, models.Org1, []urns.URN{"tel:+16055700001", "tel:+16055700003"}},
		{models.GeorgeID, models.Org1, []urns.URN{"tel:+16055743333"}},
	})
	assert.NoError(t, err)

	assertContactURNs(models.CathyID, []string{"tel:+16055700001", "tel:+16055700003"})
	assertContactURNs(models.BobID, []string{"tel:+16055742222", "tel:+16055700002"})
	assertContactURNs(models.GeorgeID, []string{"tel:+16055743333"})

	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM contacts_contacturn`, nil, numInitialURNs+3)
}
