package models_test

import (
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/nyaruka/gocommon/dbutil/assertdb"
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
	ctx, rt, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetAll)

	testdata.InsertContactURN(db, testdata.Org1, testdata.Bob, "whatsapp:250788373373", 999)
	testdata.InsertOpenTicket(db, testdata.Org1, testdata.Cathy, testdata.Zendesk, testdata.SupportTopic, "Where are my shoes?", "1234", time.Now(), testdata.Agent)
	testdata.InsertOpenTicket(db, testdata.Org1, testdata.Cathy, testdata.Zendesk, testdata.SalesTopic, "Where are my pants?", "2345", time.Now(), nil)
	testdata.InsertOpenTicket(db, testdata.Org1, testdata.Bob, testdata.Mailgun, testdata.DefaultTopic, "His name is Bob", "", time.Now(), testdata.Editor)

	// delete mailgun ticketer
	db.MustExec(`UPDATE tickets_ticketer SET is_active = false WHERE id = $1`, testdata.Mailgun.ID)

	org, err := models.GetOrgAssetsWithRefresh(ctx, rt, testdata.Org1.ID, models.RefreshAll)
	assert.NoError(t, err)

	db.MustExec(`DELETE FROM contacts_contacturn WHERE contact_id = $1`, testdata.George.ID)
	db.MustExec(`DELETE FROM contacts_contactgroup_contacts WHERE contact_id = $1`, testdata.George.ID)
	db.MustExec(`UPDATE contacts_contact SET is_active = FALSE WHERE id = $1`, testdata.Alexandria.ID)

	modelContacts, err := models.LoadContacts(ctx, db, org, []models.ContactID{testdata.Cathy.ID, testdata.Bob.ID, testdata.George.ID, testdata.Alexandria.ID})
	require.NoError(t, err)
	require.Equal(t, 3, len(modelContacts))

	// LoadContacts doesn't guarantee returned order of contacts
	sort.Slice(modelContacts, func(i, j int) bool { return modelContacts[i].ID() < modelContacts[j].ID() })

	// convert to goflow contacts
	contacts := make([]*flows.Contact, len(modelContacts))
	for i := range modelContacts {
		contacts[i], err = modelContacts[i].FlowContact(org)
		assert.NoError(t, err)
	}

	cathy, bob, george := contacts[0], contacts[1], contacts[2]

	assert.Equal(t, "Cathy", cathy.Name())
	assert.Equal(t, len(cathy.URNs()), 1)
	assert.Equal(t, cathy.URNs()[0].String(), "tel:+16055741111?id=10000&priority=1000")
	assert.Equal(t, 1, cathy.Groups().Count())
	assert.Equal(t, 2, cathy.Tickets().Count())

	cathyTickets := cathy.Tickets().All()
	assert.Equal(t, "Support", cathyTickets[0].Topic().Name())
	assert.Equal(t, "agent1@nyaruka.com", cathyTickets[0].Assignee().Email())
	assert.Equal(t, "Sales", cathyTickets[1].Topic().Name())
	assert.Nil(t, cathyTickets[1].Assignee())

	assert.Equal(t, "Yobe", cathy.Fields()["state"].QueryValue())
	assert.Equal(t, "Dokshi", cathy.Fields()["ward"].QueryValue())
	assert.Equal(t, "F", cathy.Fields()["gender"].QueryValue())
	assert.Equal(t, (*flows.FieldValue)(nil), cathy.Fields()["age"])

	assert.Equal(t, "Bob", bob.Name())
	assert.NotNil(t, bob.Fields()["joined"].QueryValue())
	assert.Equal(t, 2, len(bob.URNs()))
	assert.Equal(t, "tel:+16055742222?id=10001&priority=1000", bob.URNs()[0].String())
	assert.Equal(t, "whatsapp:250788373373?id=30000&priority=999", bob.URNs()[1].String())
	assert.Equal(t, 0, bob.Groups().Count())
	assert.Equal(t, 0, bob.Tickets().Count()) // because ticketer no longer exists

	assert.Equal(t, "George", george.Name())
	assert.Equal(t, decimal.RequireFromString("30"), george.Fields()["age"].QueryValue())
	assert.Equal(t, 0, len(george.URNs()))
	assert.Equal(t, 0, george.Groups().Count())
	assert.Equal(t, 0, george.Tickets().Count())

	// change bob to have a preferred URN and channel of our telephone
	channel := org.ChannelByID(testdata.TwilioChannel.ID)
	err = modelContacts[1].UpdatePreferredURN(ctx, db, org, testdata.Bob.URNID, channel)
	assert.NoError(t, err)

	bob, err = modelContacts[1].FlowContact(org)
	assert.NoError(t, err)
	assert.Equal(t, "tel:+16055742222?channel=74729f45-7f29-4868-9dc4-90e491e3c7d8&id=10001&priority=1000", bob.URNs()[0].String())
	assert.Equal(t, "whatsapp:250788373373?id=30000&priority=999", bob.URNs()[1].String())

	// add another tel urn to bob
	testdata.InsertContactURN(db, testdata.Org1, testdata.Bob, urns.URN("tel:+250788373373"), 10)

	// reload the contact
	modelContacts, err = models.LoadContacts(ctx, db, org, []models.ContactID{testdata.Bob.ID})
	assert.NoError(t, err)

	// set our preferred channel again
	err = modelContacts[0].UpdatePreferredURN(ctx, db, org, models.URNID(30001), channel)
	assert.NoError(t, err)

	bob, err = modelContacts[0].FlowContact(org)
	assert.NoError(t, err)
	assert.Equal(t, "tel:+250788373373?channel=74729f45-7f29-4868-9dc4-90e491e3c7d8&id=30001&priority=1000", bob.URNs()[0].String())
	assert.Equal(t, "tel:+16055742222?channel=74729f45-7f29-4868-9dc4-90e491e3c7d8&id=10001&priority=999", bob.URNs()[1].String())
	assert.Equal(t, "whatsapp:250788373373?id=30000&priority=998", bob.URNs()[2].String())

	// no op this time
	err = modelContacts[0].UpdatePreferredURN(ctx, db, org, models.URNID(30001), channel)
	assert.NoError(t, err)

	bob, err = modelContacts[0].FlowContact(org)
	assert.NoError(t, err)
	assert.Equal(t, "tel:+250788373373?channel=74729f45-7f29-4868-9dc4-90e491e3c7d8&id=30001&priority=1000", bob.URNs()[0].String())
	assert.Equal(t, "tel:+16055742222?channel=74729f45-7f29-4868-9dc4-90e491e3c7d8&id=10001&priority=999", bob.URNs()[1].String())
	assert.Equal(t, "whatsapp:250788373373?id=30000&priority=998", bob.URNs()[2].String())

	// calling with no channel is a noop on the channel
	err = modelContacts[0].UpdatePreferredURN(ctx, db, org, models.URNID(30001), nil)
	assert.NoError(t, err)

	bob, err = modelContacts[0].FlowContact(org)
	assert.NoError(t, err)
	assert.Equal(t, "tel:+250788373373?channel=74729f45-7f29-4868-9dc4-90e491e3c7d8&id=30001&priority=1000", bob.URNs()[0].String())
	assert.Equal(t, "tel:+16055742222?channel=74729f45-7f29-4868-9dc4-90e491e3c7d8&id=10001&priority=999", bob.URNs()[1].String())
	assert.Equal(t, "whatsapp:250788373373?id=30000&priority=998", bob.URNs()[2].String())
}

func TestCreateContact(t *testing.T) {
	ctx, rt, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetData)

	testdata.InsertContactGroup(db, testdata.Org1, "d636c966-79c1-4417-9f1c-82ad629773a2", "Kinyarwanda", "language = kin")

	// add an orphaned URN
	testdata.InsertContactURN(db, testdata.Org1, nil, urns.URN("telegram:200002"), 100)

	oa, err := models.GetOrgAssets(ctx, rt, testdata.Org1.ID)
	require.NoError(t, err)

	contact, flowContact, err := models.CreateContact(ctx, db, oa, models.UserID(1), "Rich", envs.Language(`kin`), []urns.URN{urns.URN("telegram:200001"), urns.URN("telegram:200002")})
	require.NoError(t, err)

	assert.Equal(t, "Rich", contact.Name())
	assert.Equal(t, envs.Language(`kin`), contact.Language())
	assert.Equal(t, []urns.URN{"telegram:200001?id=30001&priority=1000", "telegram:200002?id=30000&priority=999"}, contact.URNs())

	assert.Equal(t, "Rich", flowContact.Name())
	assert.Equal(t, envs.Language(`kin`), flowContact.Language())
	assert.Equal(t, []urns.URN{"telegram:200001?id=30001&priority=1000", "telegram:200002?id=30000&priority=999"}, flowContact.URNs().RawURNs())
	assert.Len(t, flowContact.Groups().All(), 1)
	assert.Equal(t, assets.GroupUUID("d636c966-79c1-4417-9f1c-82ad629773a2"), flowContact.Groups().All()[0].UUID())

	_, _, err = models.CreateContact(ctx, db, oa, models.UserID(1), "Rich", envs.Language(`kin`), []urns.URN{urns.URN("telegram:200001")})
	assert.EqualError(t, err, "URNs in use by other contacts")
}

func TestCreateContactRace(t *testing.T) {
	ctx, rt, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetData)

	oa, err := models.GetOrgAssets(ctx, rt, testdata.Org1.ID)
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
	ctx, rt, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetData)

	testdata.InsertContactGroup(db, testdata.Org1, "dcc16d85-8274-4d19-a3c2-152d4ee99380", "Telegrammer", `telegram = 100001`)

	// add some orphaned URNs
	testdata.InsertContactURN(db, testdata.Org1, nil, urns.URN("telegram:200001"), 100)
	testdata.InsertContactURN(db, testdata.Org1, nil, urns.URN("telegram:200002"), 100)

	contactIDSeq := models.ContactID(30000)
	newContact := func() models.ContactID { id := contactIDSeq; contactIDSeq++; return id }
	prevContact := func() models.ContactID { return contactIDSeq - 1 }

	oa, err := models.GetOrgAssets(ctx, rt, testdata.Org1.ID)
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
			testdata.Org1.ID,
			[]urns.URN{testdata.Cathy.URN},
			testdata.Cathy.ID,
			false,
			[]urns.URN{"tel:+16055741111?id=10000&priority=1000"},
			models.NilChannelID,
			[]assets.GroupUUID{testdata.DoctorsGroup.UUID},
		},
		{
			testdata.Org1.ID,
			[]urns.URN{urns.URN(testdata.Cathy.URN.String() + "?foo=bar")},
			testdata.Cathy.ID, // only URN identity is considered
			false,
			[]urns.URN{"tel:+16055741111?id=10000&priority=1000"},
			models.NilChannelID,
			[]assets.GroupUUID{testdata.DoctorsGroup.UUID},
		},
		{
			testdata.Org1.ID,
			[]urns.URN{urns.URN("telegram:100001")},
			newContact(), // creates new contact
			true,
			[]urns.URN{"telegram:100001?channel=74729f45-7f29-4868-9dc4-90e491e3c7d8&id=30002&priority=1000"},
			testdata.TwilioChannel.ID,
			[]assets.GroupUUID{"dcc16d85-8274-4d19-a3c2-152d4ee99380"},
		},
		{
			testdata.Org1.ID,
			[]urns.URN{urns.URN("telegram:100001")},
			prevContact(), // returns the same created contact
			false,
			[]urns.URN{"telegram:100001?channel=74729f45-7f29-4868-9dc4-90e491e3c7d8&id=30002&priority=1000"},
			models.NilChannelID,
			[]assets.GroupUUID{"dcc16d85-8274-4d19-a3c2-152d4ee99380"},
		},
		{
			testdata.Org1.ID,
			[]urns.URN{urns.URN("telegram:100001"), urns.URN("telegram:100002")},
			prevContact(), // same again as other URNs don't exist
			false,
			[]urns.URN{"telegram:100001?channel=74729f45-7f29-4868-9dc4-90e491e3c7d8&id=30002&priority=1000"},
			models.NilChannelID,
			[]assets.GroupUUID{"dcc16d85-8274-4d19-a3c2-152d4ee99380"},
		},
		{
			testdata.Org1.ID,
			[]urns.URN{urns.URN("telegram:100002"), urns.URN("telegram:100001")},
			prevContact(), // same again as other URNs don't exist
			false,
			[]urns.URN{"telegram:100001?channel=74729f45-7f29-4868-9dc4-90e491e3c7d8&id=30002&priority=1000"},
			models.NilChannelID,
			[]assets.GroupUUID{"dcc16d85-8274-4d19-a3c2-152d4ee99380"},
		},
		{
			testdata.Org1.ID,
			[]urns.URN{urns.URN("telegram:200001"), urns.URN("telegram:100001")},
			prevContact(), // same again as other URNs are orphaned
			false,
			[]urns.URN{"telegram:100001?channel=74729f45-7f29-4868-9dc4-90e491e3c7d8&id=30002&priority=1000"},
			models.NilChannelID,
			[]assets.GroupUUID{"dcc16d85-8274-4d19-a3c2-152d4ee99380"},
		},
		{
			testdata.Org1.ID,
			[]urns.URN{urns.URN("telegram:100003"), urns.URN("telegram:100004")}, // 2 new URNs
			newContact(),
			true,
			[]urns.URN{"telegram:100003?id=30003&priority=1000", "telegram:100004?id=30004&priority=999"},
			models.NilChannelID,
			[]assets.GroupUUID{},
		},
		{
			testdata.Org1.ID,
			[]urns.URN{urns.URN("telegram:100005"), urns.URN("telegram:200002")}, // 1 new, 1 orphaned
			newContact(),
			true,
			[]urns.URN{"telegram:100005?id=30005&priority=1000", "telegram:200002?id=30001&priority=999"},
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
	ctx, rt, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetData)

	oa, err := models.GetOrgAssets(ctx, rt, testdata.Org1.ID)
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
	ctx, rt, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetData)

	// add an orphaned URN
	testdata.InsertContactURN(db, testdata.Org1, nil, urns.URN("telegram:200001"), 100)

	contactIDSeq := models.ContactID(30000)
	newContact := func() models.ContactID { id := contactIDSeq; contactIDSeq++; return id }
	prevContact := func() models.ContactID { return contactIDSeq - 1 }

	org, err := models.GetOrgAssets(ctx, rt, testdata.Org1.ID)
	assert.NoError(t, err)

	tcs := []struct {
		OrgID      models.OrgID
		URNs       []urns.URN
		ContactIDs map[urns.URN]models.ContactID
	}{
		{
			testdata.Org1.ID,
			[]urns.URN{testdata.Cathy.URN},
			map[urns.URN]models.ContactID{testdata.Cathy.URN: testdata.Cathy.ID},
		},
		{
			testdata.Org1.ID,
			[]urns.URN{urns.URN(testdata.Cathy.URN.String() + "?foo=bar")},
			map[urns.URN]models.ContactID{urns.URN(testdata.Cathy.URN.String() + "?foo=bar"): testdata.Cathy.ID},
		},
		{
			testdata.Org1.ID,
			[]urns.URN{testdata.Cathy.URN, urns.URN("telegram:100001")},
			map[urns.URN]models.ContactID{
				testdata.Cathy.URN:          testdata.Cathy.ID,
				urns.URN("telegram:100001"): newContact(),
			},
		},
		{
			testdata.Org1.ID,
			[]urns.URN{urns.URN("telegram:100001")},
			map[urns.URN]models.ContactID{urns.URN("telegram:100001"): prevContact()},
		},
		{
			testdata.Org1.ID,
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
	ctx, rt, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetData)

	oa, err := models.GetOrgAssets(ctx, rt, testdata.Org1.ID)
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
	ctx, _, db, _ := testsuite.Get()

	ids, err := models.GetContactIDsFromReferences(ctx, db, testdata.Org1.ID, []*flows.ContactReference{
		flows.NewContactReference(testdata.Cathy.UUID, "Cathy"),
		flows.NewContactReference(testdata.Bob.UUID, "Bob"),
	})
	require.NoError(t, err)
	assert.ElementsMatch(t, []models.ContactID{testdata.Cathy.ID, testdata.Bob.ID}, ids)
}

func TestStopContact(t *testing.T) {
	ctx, _, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetAll)

	// stop kathy
	err := models.StopContact(ctx, db, testdata.Org1.ID, testdata.Cathy.ID)
	assert.NoError(t, err)

	// verify she's only in the stopped group
	assertdb.Query(t, db, `SELECT count(*) FROM contacts_contactgroup_contacts WHERE contact_id = $1`, testdata.Cathy.ID).Returns(1)

	// verify she's stopped
	assertdb.Query(t, db, `SELECT count(*) FROM contacts_contact WHERE id = $1 AND status = 'S' AND is_active = TRUE`, testdata.Cathy.ID).Returns(1)
}

func TestUpdateContactLastSeenAndModifiedOn(t *testing.T) {
	ctx, rt, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetAll)

	oa, err := models.GetOrgAssets(ctx, rt, testdata.Org1.ID)
	require.NoError(t, err)

	t0 := time.Now()

	err = models.UpdateContactModifiedOn(ctx, db, []models.ContactID{testdata.Cathy.ID})
	assert.NoError(t, err)

	assertdb.Query(t, db, `SELECT count(*) FROM contacts_contact WHERE modified_on > $1 AND last_seen_on IS NULL`, t0).Returns(1)

	t1 := time.Now().Truncate(time.Millisecond)
	time.Sleep(time.Millisecond * 5)

	err = models.UpdateContactLastSeenOn(ctx, db, testdata.Cathy.ID, t1)
	assert.NoError(t, err)

	assertdb.Query(t, db, `SELECT count(*) FROM contacts_contact WHERE modified_on > $1 AND last_seen_on = $1`, t1).Returns(1)

	cathy, err := models.LoadContact(ctx, db, oa, testdata.Cathy.ID)
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
	cathy, err = models.LoadContact(ctx, db, oa, testdata.Cathy.ID)
	require.NoError(t, err)
	assert.True(t, t2.Equal(*cathy.LastSeenOn()))
	assert.True(t, cathy.ModifiedOn().After(t2))
}

func TestUpdateContactStatus(t *testing.T) {
	ctx, _, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetAll)

	err := models.UpdateContactStatus(ctx, db, []*models.ContactStatusChange{})
	assert.NoError(t, err)

	assertdb.Query(t, db, `SELECT count(*) FROM contacts_contact WHERE id = $1 AND status = 'B'`, testdata.Cathy.ID).Returns(0)
	assertdb.Query(t, db, `SELECT count(*) FROM contacts_contact WHERE id = $1 AND status = 'S'`, testdata.Cathy.ID).Returns(0)

	changes := make([]*models.ContactStatusChange, 0, 1)
	changes = append(changes, &models.ContactStatusChange{testdata.Cathy.ID, flows.ContactStatusBlocked})

	err = models.UpdateContactStatus(ctx, db, changes)
	assert.NoError(t, err)

	assertdb.Query(t, db, `SELECT count(*) FROM contacts_contact WHERE id = $1 AND status = 'B'`, testdata.Cathy.ID).Returns(1)
	assertdb.Query(t, db, `SELECT count(*) FROM contacts_contact WHERE id = $1 AND status = 'S'`, testdata.Cathy.ID).Returns(0)

	changes = make([]*models.ContactStatusChange, 0, 1)
	changes = append(changes, &models.ContactStatusChange{testdata.Cathy.ID, flows.ContactStatusStopped})

	err = models.UpdateContactStatus(ctx, db, changes)
	assert.NoError(t, err)

	assertdb.Query(t, db, `SELECT count(*) FROM contacts_contact WHERE id = $1 AND status = 'B'`, testdata.Cathy.ID).Returns(0)
	assertdb.Query(t, db, `SELECT count(*) FROM contacts_contact WHERE id = $1 AND status = 'S'`, testdata.Cathy.ID).Returns(1)

}

func TestUpdateContactURNs(t *testing.T) {
	ctx, rt, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetAll)

	oa, err := models.GetOrgAssets(ctx, rt, testdata.Org1.ID)
	assert.NoError(t, err)

	numInitialURNs := 0
	db.Get(&numInitialURNs, `SELECT count(*) FROM contacts_contacturn`)

	assertContactURNs := func(contactID models.ContactID, expected []string) {
		var actual []string
		err = db.Select(&actual, `SELECT identity FROM contacts_contacturn WHERE contact_id = $1 ORDER BY priority DESC`, contactID)
		assert.NoError(t, err)
		assert.Equal(t, expected, actual, "URNs mismatch for contact %d", contactID)
	}

	assertContactURNs(testdata.Cathy.ID, []string{"tel:+16055741111"})
	assertContactURNs(testdata.Bob.ID, []string{"tel:+16055742222"})
	assertContactURNs(testdata.George.ID, []string{"tel:+16055743333"})

	cathyURN := urns.URN(fmt.Sprintf("tel:+16055741111?id=%d", testdata.Cathy.URNID))
	bobURN := urns.URN(fmt.Sprintf("tel:+16055742222?id=%d", testdata.Bob.URNID))

	// give Cathy a new higher priority URN
	err = models.UpdateContactURNs(ctx, db, oa, []*models.ContactURNsChanged{{testdata.Cathy.ID, testdata.Org1.ID, []urns.URN{"tel:+16055700001", cathyURN}}})
	assert.NoError(t, err)

	assertContactURNs(testdata.Cathy.ID, []string{"tel:+16055700001", "tel:+16055741111"})

	// give Bob a new lower priority URN
	err = models.UpdateContactURNs(ctx, db, oa, []*models.ContactURNsChanged{{testdata.Bob.ID, testdata.Org1.ID, []urns.URN{bobURN, "tel:+16055700002"}}})
	assert.NoError(t, err)

	assertContactURNs(testdata.Bob.ID, []string{"tel:+16055742222", "tel:+16055700002"})
	assertdb.Query(t, db, `SELECT count(*) FROM contacts_contacturn WHERE contact_id IS NULL`).Returns(0) // shouldn't be any orphan URNs
	assertdb.Query(t, db, `SELECT count(*) FROM contacts_contacturn`).Returns(numInitialURNs + 2)         // but 2 new URNs

	// remove a URN from Cathy
	err = models.UpdateContactURNs(ctx, db, oa, []*models.ContactURNsChanged{{testdata.Cathy.ID, testdata.Org1.ID, []urns.URN{"tel:+16055700001"}}})
	assert.NoError(t, err)

	assertContactURNs(testdata.Cathy.ID, []string{"tel:+16055700001"})
	assertdb.Query(t, db, `SELECT count(*) FROM contacts_contacturn WHERE contact_id IS NULL`).Returns(1) // now orphaned

	// steal a URN from Bob
	err = models.UpdateContactURNs(ctx, db, oa, []*models.ContactURNsChanged{{testdata.Cathy.ID, testdata.Org1.ID, []urns.URN{"tel:+16055700001", "tel:+16055700002"}}})
	assert.NoError(t, err)

	assertContactURNs(testdata.Cathy.ID, []string{"tel:+16055700001", "tel:+16055700002"})
	assertContactURNs(testdata.Bob.ID, []string{"tel:+16055742222"})

	// steal the URN back from Cathy whilst simulataneously adding new URN to Cathy and not-changing anything for George
	err = models.UpdateContactURNs(ctx, db, oa, []*models.ContactURNsChanged{
		{testdata.Bob.ID, testdata.Org1.ID, []urns.URN{"tel:+16055742222", "tel:+16055700002"}},
		{testdata.Cathy.ID, testdata.Org1.ID, []urns.URN{"tel:+16055700001", "tel:+16055700003"}},
		{testdata.George.ID, testdata.Org1.ID, []urns.URN{"tel:+16055743333"}},
	})
	assert.NoError(t, err)

	assertContactURNs(testdata.Cathy.ID, []string{"tel:+16055700001", "tel:+16055700003"})
	assertContactURNs(testdata.Bob.ID, []string{"tel:+16055742222", "tel:+16055700002"})
	assertContactURNs(testdata.George.ID, []string{"tel:+16055743333"})

	assertdb.Query(t, db, `SELECT count(*) FROM contacts_contacturn`).Returns(numInitialURNs + 3)
}
