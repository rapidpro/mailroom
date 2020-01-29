package models

import (
	"testing"

	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/search"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/olivere/elastic"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"

	"fmt"
)

func TestElasticContacts(t *testing.T) {
	testsuite.Reset()
	ctx := testsuite.CTX()
	db := testsuite.DB()

	es := search.NewMockElasticServer()
	defer es.Close()

	client, err := elastic.NewClient(
		elastic.SetURL(es.URL()),
		elastic.SetHealthcheck(false),
		elastic.SetSniff(false),
	)
	assert.NoError(t, err)

	org, err := GetOrgAssets(ctx, db, 1)
	assert.NoError(t, err)

	tcs := []struct {
		Query    string
		Request  string
		Response string
		Contacts []ContactID
		Error    bool
	}{
		{
			Query: "george",
			Request: `{
				"_source":false,
				"query":{
					"bool":{
						"must":[
							{ "bool":{
								"must":[
									{"term":{"org_id":1}},
									{"term":{"is_active":true}},
									{"match":{"name":{"query":"george"}}}
								]
							}},
							{ "term":{
								"is_blocked":false
							}},
							{"term":
								{"is_stopped":false
							}}
						]
					}
				},
				"sort":["_doc"]
			}`,
			Response: fmt.Sprintf(`{
				"_scroll_id": "DXF1ZXJ5QW5kRmV0Y2gBAAAAAAAbgc0WS1hqbHlfb01SM2lLTWJRMnVOSVZDdw==",
				"took": 2,
				"timed_out": false,
				"_shards": {
				  "total": 1,
				  "successful": 1,
				  "skipped": 0,
				  "failed": 0
				},
				"hits": {
				  "total": 1,
				  "max_score": null,
				  "hits": [
					{
					  "_index": "contacts",
					  "_type": "_doc",
					  "_id": "%d",
					  "_score": null,
					  "_routing": "1",
					  "sort": [
						15124352
					  ]
					}
				  ]
				}
			}`, GeorgeID),
			Contacts: []ContactID{GeorgeID},
		}, {
			Query: "nobody",
			Request: `{
				"_source":false,
				"query":{
					"bool":{
						"must":[
							{"bool":
								{"must":[
									{"term":{"org_id":1}},
									{"term":{"is_active":true}},
									{"match":{"name":{"query":"nobody"}}}
								]}
							},
							{"term":{"is_blocked":false}},
							{"term":{"is_stopped":false}}
						]
					}
				},
				"sort":["_doc"]
			}`,
			Response: `{
				"_scroll_id": "DXF1ZXJ5QW5kRmV0Y2gBAAAAAAAbgc0WS1hqbHlfb01SM2lLTWJRMnVOSVZDdw==",
				"took": 2,
				"timed_out": false,
				"_shards": {
				  "total": 1,
				  "successful": 1,
				  "skipped": 0,
				  "failed": 0
				},
				"hits": {
				  "total": 0,
				  "max_score": null,
				  "hits": []
				}
			}`,
			Contacts: []ContactID{},
		}, {
			Query: "goats > 2", // no such contact field
			Error: true,
		},
	}

	for i, tc := range tcs {
		es.NextResponse = tc.Response

		ids, err := ContactIDsForQuery(ctx, client, org, tc.Query)

		if tc.Error {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err, "%d: error encountered performing query", i)
			assert.JSONEq(t, tc.Request, es.LastBody, "%d: request mismatch, got: %s", i, es.LastBody)
			assert.Equal(t, tc.Contacts, ids, "%d: ids mismatch", i)
		}
	}
}

func TestContacts(t *testing.T) {
	testsuite.Reset()
	ctx := testsuite.CTX()
	db := testsuite.DB()

	org, err := GetOrgAssets(ctx, db, 1)
	assert.NoError(t, err)

	session, err := NewSessionAssets(org)
	assert.NoError(t, err)

	db.MustExec(
		`INSERT INTO contacts_contacturn(org_id, contact_id, scheme, path, identity, priority) 
		                          VALUES(1, $1, 'whatsapp', '250788373373', 'whatsapp:250788373373', 100)`, BobID)

	db.MustExec(`DELETE FROM contacts_contacturn WHERE contact_id = $1`, GeorgeID)
	db.MustExec(`DELETE FROM contacts_contactgroup_contacts WHERE contact_id = $1`, GeorgeID)
	db.MustExec(`UPDATE contacts_contact SET is_active = FALSE WHERE id = $1`, AlexandriaID)

	modelContacts, err := LoadContacts(ctx, db, org, []ContactID{CathyID, GeorgeID, BobID, AlexandriaID})
	assert.NoError(t, err)
	assert.Equal(t, 3, len(modelContacts))

	// convert to goflow contacts
	contacts := make([]*flows.Contact, len(modelContacts))
	for i := range modelContacts {
		contacts[i], err = modelContacts[i].FlowContact(org, session)
		assert.NoError(t, err)
	}

	if len(contacts) == 3 {
		assert.Equal(t, "Cathy", contacts[0].Name())
		assert.Equal(t, len(contacts[0].URNs()), 1)
		assert.Equal(t, contacts[0].URNs()[0].String(), "tel:+250700000001?id=10000&priority=50")
		assert.Equal(t, 1, contacts[0].Groups().Count())

		assert.Equal(t, "Sokoto", contacts[0].Fields()["state"].QueryValue())
		assert.Equal(t, "Kilgori", contacts[0].Fields()["ward"].QueryValue())
		assert.Equal(t, "F", contacts[0].Fields()["gender"].QueryValue())
		assert.Equal(t, (*flows.FieldValue)(nil), contacts[0].Fields()["age"])

		assert.Equal(t, "Bob", contacts[1].Name())
		assert.NotNil(t, contacts[1].Fields()["joined"].QueryValue())
		assert.Equal(t, 2, len(contacts[1].URNs()))
		assert.Equal(t, contacts[1].URNs()[0].String(), "whatsapp:250788373373?id=20121&priority=100")
		assert.Equal(t, contacts[1].URNs()[1].String(), "tel:+250700000002?id=10001&priority=50")
		assert.Equal(t, 0, contacts[1].Groups().Count())

		assert.Equal(t, "George", contacts[2].Name())
		assert.Equal(t, decimal.RequireFromString("30"), contacts[2].Fields()["age"].QueryValue())
		assert.Equal(t, 0, len(contacts[2].URNs()))
		assert.Equal(t, 0, contacts[2].Groups().Count())
	}

	// change bob to have a preferred URN and channel of our telephone
	channel := org.ChannelByID(TwilioChannelID)
	err = modelContacts[1].UpdatePreferredURN(ctx, db, org, BobURNID, channel)
	assert.NoError(t, err)

	bob, err := modelContacts[1].FlowContact(org, session)
	assert.NoError(t, err)
	assert.Equal(t, "tel:+250700000002?channel=74729f45-7f29-4868-9dc4-90e491e3c7d8&id=10001&priority=1000", bob.URNs()[0].String())
	assert.Equal(t, "whatsapp:250788373373?id=20121&priority=999", bob.URNs()[1].String())

	// add another tel urn to bob
	db.MustExec(
		`INSERT INTO contacts_contacturn(org_id, contact_id, scheme, path, identity, priority) 
		                          VALUES(1, $1, 'tel', '+250788373393', 'tel:+250788373373', 10)`, BobID)

	// reload the contact
	modelContacts, err = LoadContacts(ctx, db, org, []ContactID{BobID})
	assert.NoError(t, err)

	// set our preferred channel again
	err = modelContacts[0].UpdatePreferredURN(ctx, db, org, URNID(20122), channel)
	assert.NoError(t, err)

	bob, err = modelContacts[0].FlowContact(org, session)
	assert.NoError(t, err)
	assert.Equal(t, "tel:+250788373393?channel=74729f45-7f29-4868-9dc4-90e491e3c7d8&id=20122&priority=1000", bob.URNs()[0].String())
	assert.Equal(t, "tel:+250700000002?channel=74729f45-7f29-4868-9dc4-90e491e3c7d8&id=10001&priority=999", bob.URNs()[1].String())
	assert.Equal(t, "whatsapp:250788373373?id=20121&priority=998", bob.URNs()[2].String())

	// no op this time
	err = modelContacts[0].UpdatePreferredURN(ctx, db, org, URNID(20122), channel)
	assert.NoError(t, err)

	bob, err = modelContacts[0].FlowContact(org, session)
	assert.NoError(t, err)
	assert.Equal(t, "tel:+250788373393?channel=74729f45-7f29-4868-9dc4-90e491e3c7d8&id=20122&priority=1000", bob.URNs()[0].String())
	assert.Equal(t, "tel:+250700000002?channel=74729f45-7f29-4868-9dc4-90e491e3c7d8&id=10001&priority=999", bob.URNs()[1].String())
	assert.Equal(t, "whatsapp:250788373373?id=20121&priority=998", bob.URNs()[2].String())

	// calling with no channel is a noop on the channel
	err = modelContacts[0].UpdatePreferredURN(ctx, db, org, URNID(20122), nil)
	assert.NoError(t, err)

	bob, err = modelContacts[0].FlowContact(org, session)
	assert.NoError(t, err)
	assert.Equal(t, "tel:+250788373393?channel=74729f45-7f29-4868-9dc4-90e491e3c7d8&id=20122&priority=1000", bob.URNs()[0].String())
	assert.Equal(t, "tel:+250700000002?channel=74729f45-7f29-4868-9dc4-90e491e3c7d8&id=10001&priority=999", bob.URNs()[1].String())
	assert.Equal(t, "whatsapp:250788373373?id=20121&priority=998", bob.URNs()[2].String())
}

func TestContactsFromURN(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()
	testsuite.Reset()

	var maxContactID int64
	db.Get(&maxContactID, `SELECT max(id) FROM contacts_contact`)

	tcs := []struct {
		OrgID     OrgID
		URN       urns.URN
		ContactID ContactID
	}{
		{Org1, CathyURN, CathyID},
		{Org1, urns.URN(CathyURN.String() + "?foo=bar"), CathyID},
		{Org1, urns.URN("telegram:12345678"), ContactID(maxContactID + 1)},
		{Org1, urns.URN("telegram:12345678"), ContactID(maxContactID + 1)},
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

	var maxContactID int64
	db.Get(&maxContactID, `SELECT max(id) FROM contacts_contact`)

	tcs := []struct {
		OrgID     OrgID
		URN       urns.URN
		ContactID ContactID
	}{
		{Org1, CathyURN, CathyID},
		{Org1, urns.URN(CathyURN.String() + "?foo=bar"), CathyID},
		{Org1, urns.URN("telegram:12345678"), ContactID(maxContactID + 3)},
		{Org1, urns.URN("telegram:12345678"), ContactID(maxContactID + 3)},
		{Org1, urns.NilURN, ContactID(maxContactID + 5)},
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

	// stop kathy
	err = StopContact(ctx, db, Org1, CathyID)
	assert.NoError(t, err)

	// verify she's only in the stopped group
	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM contacts_contactgroup_contacts WHERE contact_id = $1`, []interface{}{CathyID}, 1)

	// verify she's stopped
	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM contacts_contact WHERE id = $1 AND is_stopped = TRUE AND is_active = TRUE and is_blocked = FALSE`, []interface{}{CathyID}, 1)
}
