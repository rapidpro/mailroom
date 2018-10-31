package models

import (
	"testing"

	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/excellent/types"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/engine"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
)

const (
	Org1 = OrgID(1)

	Cathy      = flows.ContactID(43)
	CathyURN   = urns.URN("tel:+250700000002")
	CathyURNID = URNID(43)

	Bob      = flows.ContactID(58)
	BobURN   = urns.URN("tel:+250700000017")
	BobURNID = URNID(59)

	Evan = flows.ContactID(47)
)

func TestContacts(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()

	org, err := GetOrgAssets(ctx, db, 1)
	assert.NoError(t, err)

	session, err := engine.NewSessionAssets(org)
	assert.NoError(t, err)

	db.MustExec(
		`INSERT INTO contacts_contacturn(org_id, contact_id, scheme, path, identity, priority) 
		                          VALUES(1, 80, 'whatsapp', '250788373373', 'whatsapp:250788373373', 100)`)

	db.MustExec(`DELETE FROM contacts_contacturn WHERE contact_id = 43`)
	db.MustExec(`DELETE FROM contacts_contactgroup_contacts WHERE contact_id = 43`)
	db.MustExec(`UPDATE contacts_contact SET is_active = FALSE WHERE id = 82`)

	modelContacts, err := LoadContacts(ctx, db, org, []flows.ContactID{1, 42, 43, 80, 82})
	assert.NoError(t, err)
	assert.Equal(t, 3, len(modelContacts))

	// convert to goflow contacts
	contacts := make([]*flows.Contact, len(modelContacts))
	for i := range modelContacts {
		contacts[i], err = modelContacts[i].FlowContact(org, session)
		assert.NoError(t, err)
	}

	if len(contacts) == 3 {
		assert.Equal(t, "Cathy Quincy", contacts[0].Name())
		assert.Equal(t, len(contacts[0].URNs()), 1)
		assert.Equal(t, contacts[0].URNs()[0].String(), "tel:+250700000001?id=42&priority=50")
		assert.Equal(t, 5, contacts[0].Groups().Length())

		assert.Equal(t, flows.LocationPath("Nigeria > Sokoto"), contacts[0].Fields()["state"].TypedValue())
		assert.Equal(t, flows.LocationPath("Nigeria > Sokoto > Yabo > Kilgori"), contacts[0].Fields()["ward"].TypedValue())
		assert.Equal(t, types.NewXText("F"), contacts[0].Fields()["gender"].TypedValue())
		assert.Equal(t, (*flows.FieldValue)(nil), contacts[0].Fields()["age"])

		assert.Equal(t, "Dave Jameson", contacts[1].Name())
		assert.Equal(t, types.NewXNumber(decimal.RequireFromString("30")), contacts[1].Fields()["age"].TypedValue())
		assert.Equal(t, 0, len(contacts[1].URNs()))
		assert.Equal(t, 0, contacts[1].Groups().Length())

		assert.Equal(t, "Cathy Roberts", contacts[2].Name())
		assert.NotNil(t, contacts[2].Fields()["joined"].TypedValue())
		assert.Equal(t, 2, len(contacts[2].URNs()))
		assert.Equal(t, contacts[2].URNs()[0].String(), "whatsapp:250788373373?id=10044&priority=100")
		assert.Equal(t, contacts[2].URNs()[1].String(), "tel:+250700000039?id=82&priority=50")
		assert.Equal(t, 2, contacts[2].Groups().Length())
	}

	// change cathy to have a preferred URN and channel of our telephone
	channel := org.ChannelByID(ChannelID(2))
	err = modelContacts[2].UpdatePreferredURN(ctx, db, org, URNID(82), channel)
	assert.NoError(t, err)

	cathy, err := modelContacts[2].FlowContact(org, session)
	assert.NoError(t, err)
	assert.Equal(t, cathy.URNs()[0].String(), "tel:+250700000039?channel=c534272e-817d-4a78-a70c-f21df34407f8&id=82&priority=1000")
	assert.Equal(t, cathy.URNs()[1].String(), "whatsapp:250788373373?id=10044&priority=999")

	// add another tel urn to cathy
	db.MustExec(
		`INSERT INTO contacts_contacturn(org_id, contact_id, scheme, path, identity, priority) 
		                          VALUES(1, 80, 'tel', '+250788373393', 'tel:+250788373373', 10)`)

	// reload the contact
	modelContacts, err = LoadContacts(ctx, db, org, []flows.ContactID{80})
	assert.NoError(t, err)

	// set our preferred channel again
	err = modelContacts[0].UpdatePreferredURN(ctx, db, org, URNID(10045), channel)
	assert.NoError(t, err)

	cathy, err = modelContacts[0].FlowContact(org, session)
	assert.NoError(t, err)
	assert.Equal(t, cathy.URNs()[0].String(), "tel:+250788373393?channel=c534272e-817d-4a78-a70c-f21df34407f8&id=10045&priority=1000")
	assert.Equal(t, cathy.URNs()[1].String(), "tel:+250700000039?channel=c534272e-817d-4a78-a70c-f21df34407f8&id=82&priority=999")
	assert.Equal(t, cathy.URNs()[2].String(), "whatsapp:250788373373?id=10044&priority=998")

	// no op this time
	err = modelContacts[0].UpdatePreferredURN(ctx, db, org, URNID(10045), channel)
	assert.NoError(t, err)

	cathy, err = modelContacts[0].FlowContact(org, session)
	assert.NoError(t, err)
	assert.Equal(t, cathy.URNs()[0].String(), "tel:+250788373393?channel=c534272e-817d-4a78-a70c-f21df34407f8&id=10045&priority=1000")
	assert.Equal(t, cathy.URNs()[1].String(), "tel:+250700000039?channel=c534272e-817d-4a78-a70c-f21df34407f8&id=82&priority=999")
	assert.Equal(t, cathy.URNs()[2].String(), "whatsapp:250788373373?id=10044&priority=998")
}

func TestContactsFromURN(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()
	testsuite.Reset()

	tcs := []struct {
		OrgID     OrgID
		URN       urns.URN
		ContactID flows.ContactID
	}{
		{Org1, CathyURN, Cathy},
		{Org1, urns.URN(CathyURN.String() + "?foo=bar"), Cathy},
		{Org1, urns.URN("telegram:12345678"), 10041},
		{Org1, urns.URN("telegram:12345678"), 10041},
	}

	org, err := GetOrgAssets(ctx, db, Org1)
	assert.NoError(t, err)

	assets, err := GetSessionAssets(org)
	assert.NoError(t, err)

	for i, tc := range tcs {
		ids, err := ContactIDsFromURNs(ctx, db, org, assets, []urns.URN{tc.URN})
		assert.NoError(t, err, "%d: error getting contact ids", i)

		if len(ids) != 1 {
			assert.Fail(t, "%d: unexpected number of ids returned", i)
			continue
		}
		assert.Equal(t, tc.ContactID, ids[tc.URN], "%d: mismatch in contact ids", i)
	}
}

func TestCreateContact(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()
	testsuite.Reset()

	tcs := []struct {
		OrgID     OrgID
		URN       urns.URN
		ContactID flows.ContactID
	}{
		{Org1, CathyURN, Cathy},
		{Org1, urns.URN(CathyURN.String() + "?foo=bar"), Cathy},
		{Org1, urns.URN("telegram:12345678"), 10043},
		{Org1, urns.URN("telegram:12345678"), 10043},
		{Org1, urns.NilURN, 10045},
	}

	org, err := GetOrgAssets(ctx, db, Org1)
	assert.NoError(t, err)

	assets, err := GetSessionAssets(org)
	assert.NoError(t, err)

	for i, tc := range tcs {
		id, err := CreateContact(ctx, db, org, assets, tc.URN)
		assert.NoError(t, err, "%d: error creating contact", i)
		assert.Equal(t, tc.ContactID, id, "%d: mismatch in contact id", i)
	}
}
