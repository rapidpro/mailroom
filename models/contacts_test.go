package models

import (
	"fmt"
	"testing"
	"time"

	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/test"
	"github.com/nyaruka/mailroom/testsuite"

	"github.com/olivere/elastic"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestContactIDsForQueryPage(t *testing.T) {
	testsuite.Reset()
	ctx := testsuite.CTX()
	db := testsuite.DB()

	es := testsuite.NewMockElasticServer()
	defer es.Close()

	client, err := elastic.NewClient(
		elastic.SetURL(es.URL()),
		elastic.SetHealthcheck(false),
		elastic.SetSniff(false),
	)
	require.NoError(t, err)

	oa, err := GetOrgAssets(ctx, db, 1)
	require.NoError(t, err)

	tcs := []struct {
		Group             assets.GroupUUID
		ExcludeIDs        []ContactID
		Query             string
		Sort              string
		ExpectedESRequest string
		MockedESResponse  string
		ExpectedContacts  []ContactID
		ExpectedTotal     int64
		ExpectedError     string
	}{
		{
			Group: AllContactsGroupUUID,
			Query: "george",
			ExpectedESRequest: `{
				"_source": false,
				"from": 0,
				"query": {
					"bool": {
						"must": [
							{
								"term": {
									"org_id": 1
								}
							},
							{
								"term": {
									"is_active": true
								}
							},
							{
								"term": {
									"groups": "d1ee73f0-bdb5-47ce-99dd-0c95d4ebf008"
								}
							},
							{
								"match": {
									"name": {
										"query": "george"
									}
								}
							}
						]
					}
				},
				"size": 50,
				"sort": [
					{
						"id": {
							"order": "desc"
						}
					}
				]
			}`,
			MockedESResponse: fmt.Sprintf(`{
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
			ExpectedContacts: []ContactID{GeorgeID},
			ExpectedTotal:    1,
		},
		{
			Group:      BlockedContactsGroupUUID,
			ExcludeIDs: []ContactID{BobID, CathyID},
			Query:      "age > 32",
			Sort:       "-age",
			ExpectedESRequest: `{
				"_source": false,
				"from": 0,
				"query": {
					"bool": {
						"must": [
							{
								"term": {
									"org_id": 1
								}
							},
							{
								"term": {
									"is_active": true
								}
							},
							{
								"term": {
									"groups": "9295ebab-5c2d-4eb1-86f9-7c15ed2f3219"
								}
							},
							{
								"nested": {
									"path": "fields",
									"query": {
										"bool": {
											"must": [
												{
													"term": {
														"fields.field": "903f51da-2717-47c7-a0d3-f2f32877013d"
													}
												},
												{
													"range": {
														"fields.number": {
															"from": 32,
															"include_lower": false,
															"include_upper": true,
															"to": null
														}
													}
												}
											]
										}
									}
								}
							}
						],
						"must_not": {
							"ids": {
								"type": "_doc",
								"values": [
									"10001",
									"10000"
								]
							}
						}
					}
				},
				"size": 50,
				"sort": [
					{
						"fields.number": {
							"nested": {
								"filter": {
									"term": {
										"fields.field": "903f51da-2717-47c7-a0d3-f2f32877013d"
									}
								},
								"path": "fields"
							},
							"order": "desc"
						}
					}
				]
			}`,
			MockedESResponse: fmt.Sprintf(`{
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
			ExpectedContacts: []ContactID{GeorgeID},
			ExpectedTotal:    1,
		},
		{
			Query:         "goats > 2", // no such contact field
			ExpectedError: "error parsing query: goats > 2: can't resolve 'goats' to attribute, scheme or field",
		},
	}

	for i, tc := range tcs {
		es.NextResponse = tc.MockedESResponse

		_, ids, total, err := ContactIDsForQueryPage(ctx, client, oa, tc.Group, tc.ExcludeIDs, tc.Query, tc.Sort, 0, 50)

		if tc.ExpectedError != "" {
			assert.EqualError(t, err, tc.ExpectedError)
		} else {
			assert.NoError(t, err, "%d: error encountered performing query", i)
			assert.Equal(t, tc.ExpectedContacts, ids, "%d: ids mismatch", i)
			assert.Equal(t, tc.ExpectedTotal, total, "%d: total mismatch", i)

			test.AssertEqualJSON(t, []byte(tc.ExpectedESRequest), []byte(es.LastBody), "%d: ES request mismatch", i)
		}
	}
}

func TestContactIDsForQuery(t *testing.T) {
	testsuite.Reset()
	ctx := testsuite.CTX()
	db := testsuite.DB()

	es := testsuite.NewMockElasticServer()
	defer es.Close()

	client, err := elastic.NewClient(
		elastic.SetURL(es.URL()),
		elastic.SetHealthcheck(false),
		elastic.SetSniff(false),
	)
	require.NoError(t, err)

	oa, err := GetOrgAssets(ctx, db, 1)
	require.NoError(t, err)

	tcs := []struct {
		Query             string
		ExpectedESRequest string
		MockedESResponse  string
		ExpectedContacts  []ContactID
		ExpectedError     string
	}{
		{
			Query: "george",
			ExpectedESRequest: `{
				"_source":false,
				"query": {
					"bool": {
						"must": [
							{
								"term": {
									"org_id": 1
								}
							},
							{
								"term": {
									"is_active": true
								}
							},
							{
								"term": {
									"status": "A"
								}
							},
							{
								"match": {
									"name": {
										"query": "george"
									}
								}
							}
						]
					}
				},
				"sort":["_doc"]
			}`,
			MockedESResponse: fmt.Sprintf(`{
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
			ExpectedContacts: []ContactID{GeorgeID},
		}, {
			Query: "nobody",
			ExpectedESRequest: `{
				"_source":false,
				"query": {
					"bool": {
						"must": [
							{
								"term": {
									"org_id": 1
								}
							},
							{
								"term": {
									"is_active": true
								}
							},
							{
								"term": {
									"status": "A"
								}
							},
							{
								"match": {
									"name": {
										"query": "nobody"
									}
								}
							}
						]
					}
				},
				"sort":["_doc"]
			}`,
			MockedESResponse: `{
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
			ExpectedContacts: []ContactID{},
		},
		{
			Query:         "goats > 2", // no such contact field
			ExpectedError: "error parsing query: goats > 2: can't resolve 'goats' to attribute, scheme or field",
		},
	}

	for i, tc := range tcs {
		es.NextResponse = tc.MockedESResponse

		ids, err := ContactIDsForQuery(ctx, client, oa, tc.Query)

		if tc.ExpectedError != "" {
			assert.EqualError(t, err, tc.ExpectedError)
		} else {
			assert.NoError(t, err, "%d: error encountered performing query", i)
			assert.Equal(t, tc.ExpectedContacts, ids, "%d: ids mismatch", i)

			test.AssertEqualJSON(t, []byte(tc.ExpectedESRequest), []byte(es.LastBody), "%d: request mismatch", i)
		}
	}
}

func TestContacts(t *testing.T) {
	testsuite.Reset()
	ctx := testsuite.CTX()
	db := testsuite.DB()

	org, err := GetOrgAssets(ctx, db, 1)
	assert.NoError(t, err)

	db.MustExec(
		`INSERT INTO contacts_contacturn(org_id, contact_id, scheme, path, identity, priority) 
		                          VALUES(1, $1, 'whatsapp', '250788373373', 'whatsapp:250788373373', 999)`, BobID)

	db.MustExec(`DELETE FROM contacts_contacturn WHERE contact_id = $1`, GeorgeID)
	db.MustExec(`DELETE FROM contacts_contactgroup_contacts WHERE contact_id = $1`, GeorgeID)
	db.MustExec(`UPDATE contacts_contact SET is_active = FALSE WHERE id = $1`, AlexandriaID)

	modelContacts, err := LoadContacts(ctx, db, org, []ContactID{CathyID, GeorgeID, BobID, AlexandriaID})
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
	channel := org.ChannelByID(TwilioChannelID)
	err = modelContacts[1].UpdatePreferredURN(ctx, db, org, BobURNID, channel)
	assert.NoError(t, err)

	bob, err := modelContacts[1].FlowContact(org)
	assert.NoError(t, err)
	assert.Equal(t, "tel:+16055742222?channel=74729f45-7f29-4868-9dc4-90e491e3c7d8&id=10001&priority=1000", bob.URNs()[0].String())
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

	bob, err = modelContacts[0].FlowContact(org)
	assert.NoError(t, err)
	assert.Equal(t, "tel:+250788373393?channel=74729f45-7f29-4868-9dc4-90e491e3c7d8&id=20122&priority=1000", bob.URNs()[0].String())
	assert.Equal(t, "tel:+16055742222?channel=74729f45-7f29-4868-9dc4-90e491e3c7d8&id=10001&priority=999", bob.URNs()[1].String())
	assert.Equal(t, "whatsapp:250788373373?id=20121&priority=998", bob.URNs()[2].String())

	// no op this time
	err = modelContacts[0].UpdatePreferredURN(ctx, db, org, URNID(20122), channel)
	assert.NoError(t, err)

	bob, err = modelContacts[0].FlowContact(org)
	assert.NoError(t, err)
	assert.Equal(t, "tel:+250788373393?channel=74729f45-7f29-4868-9dc4-90e491e3c7d8&id=20122&priority=1000", bob.URNs()[0].String())
	assert.Equal(t, "tel:+16055742222?channel=74729f45-7f29-4868-9dc4-90e491e3c7d8&id=10001&priority=999", bob.URNs()[1].String())
	assert.Equal(t, "whatsapp:250788373373?id=20121&priority=998", bob.URNs()[2].String())

	// calling with no channel is a noop on the channel
	err = modelContacts[0].UpdatePreferredURN(ctx, db, org, URNID(20122), nil)
	assert.NoError(t, err)

	bob, err = modelContacts[0].FlowContact(org)
	assert.NoError(t, err)
	assert.Equal(t, "tel:+250788373393?channel=74729f45-7f29-4868-9dc4-90e491e3c7d8&id=20122&priority=1000", bob.URNs()[0].String())
	assert.Equal(t, "tel:+16055742222?channel=74729f45-7f29-4868-9dc4-90e491e3c7d8&id=10001&priority=999", bob.URNs()[1].String())
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

	for i, tc := range tcs {
		ids, err := ContactIDsFromURNs(ctx, db, org, []urns.URN{tc.URN})
		assert.NoError(t, err, "%d: error getting contact ids", i)

		if len(ids) != 1 {
			assert.Fail(t, "%d: unexpected number of ids returned", i)
			continue
		}
		assert.Equal(t, tc.ContactID, ids[tc.URN], "%d: mismatch in contact ids", i)
	}
}

func TestGetOrCreateContact(t *testing.T) {
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
	}

	org, err := GetOrgAssets(ctx, db, Org1)
	assert.NoError(t, err)

	for i, tc := range tcs {
		contact, _, err := GetOrCreateContact(ctx, db, org, tc.URN)
		assert.NoError(t, err, "%d: error creating contact", i)
		assert.Equal(t, tc.ContactID, contact.ID(), "%d: mismatch in contact id", i)
	}
}

func TestStopContact(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()

	// stop kathy
	err := StopContact(ctx, db, Org1, CathyID)
	assert.NoError(t, err)

	// verify she's only in the stopped group
	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM contacts_contactgroup_contacts WHERE contact_id = $1`, []interface{}{CathyID}, 1)

	// verify she's stopped
	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM contacts_contact WHERE id = $1 AND status = 'S' AND is_active = TRUE`, []interface{}{CathyID}, 1)
}

func TestUpdateContactLastSeenAndModifiedOn(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()
	testsuite.Reset()

	oa, err := GetOrgAssets(ctx, db, Org1)
	require.NoError(t, err)

	t0 := time.Now()

	err = UpdateContactModifiedOn(ctx, db, []ContactID{CathyID})
	assert.NoError(t, err)

	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM contacts_contact WHERE modified_on > $1 AND last_seen_on IS NULL`, []interface{}{t0}, 1)

	t1 := time.Now().Truncate(time.Millisecond)
	time.Sleep(time.Millisecond * 5)

	err = UpdateContactLastSeenOn(ctx, db, CathyID, t1)
	assert.NoError(t, err)

	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM contacts_contact WHERE modified_on > $1 AND last_seen_on = $1`, []interface{}{t1}, 1)

	cathy, err := LoadContact(ctx, db, oa, CathyID)
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
	cathy, err = LoadContact(ctx, db, oa, CathyID)
	require.NoError(t, err)
	assert.True(t, t2.Equal(*cathy.LastSeenOn()))
	assert.True(t, cathy.ModifiedOn().After(t2))
}

func TestUpdateContactModifiedBy(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()
	testsuite.Reset()

	err := UpdateContactModifiedBy(ctx, db, []ContactID{}, UserID(0))
	assert.NoError(t, err)

	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM contacts_contact WHERE id = $1 AND modified_by_id = $2`, []interface{}{CathyID, UserID(0)}, 0)

	err = UpdateContactModifiedBy(ctx, db, []ContactID{CathyID}, UserID(0))
	assert.NoError(t, err)

	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM contacts_contact WHERE id = $1 AND modified_by_id = $2`, []interface{}{CathyID, UserID(0)}, 0)

	err = UpdateContactModifiedBy(ctx, db, []ContactID{CathyID}, UserID(1))
	assert.NoError(t, err)

	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM contacts_contact WHERE id = $1 AND modified_by_id = $2`, []interface{}{CathyID, UserID(1)}, 1)
}

func TestUpdateContactStatus(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()
	testsuite.Reset()

	err := UpdateContactStatus(ctx, db, []*ContactStatusChange{})
	assert.NoError(t, err)

	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM contacts_contact WHERE id = $1 AND status = 'B'`, []interface{}{CathyID}, 0)
	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM contacts_contact WHERE id = $1 AND status = 'S'`, []interface{}{CathyID}, 0)

	changes := make([]*ContactStatusChange, 0, 1)
	changes = append(changes, &ContactStatusChange{CathyID, flows.ContactStatusBlocked})

	err = UpdateContactStatus(ctx, db, changes)

	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM contacts_contact WHERE id = $1 AND status = 'B'`, []interface{}{CathyID}, 1)
	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM contacts_contact WHERE id = $1 AND status = 'S'`, []interface{}{CathyID}, 0)

	changes = make([]*ContactStatusChange, 0, 1)
	changes = append(changes, &ContactStatusChange{CathyID, flows.ContactStatusStopped})

	err = UpdateContactStatus(ctx, db, changes)

	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM contacts_contact WHERE id = $1 AND status = 'B'`, []interface{}{CathyID}, 0)
	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM contacts_contact WHERE id = $1 AND status = 'S'`, []interface{}{CathyID}, 1)

}

func TestUpdateContactURNs(t *testing.T) {
	testsuite.Reset()
	ctx := testsuite.CTX()
	db := testsuite.DB()
	testsuite.Reset()

	oa, err := GetOrgAssets(ctx, db, Org1)
	assert.NoError(t, err)

	numInitialURNs := 0
	db.Get(&numInitialURNs, `SELECT count(*) FROM contacts_contacturn`)

	assertContactURNs := func(contactID ContactID, expected []string) {
		var actual []string
		err = db.Select(&actual, `SELECT identity FROM contacts_contacturn WHERE contact_id = $1 ORDER BY priority DESC`, contactID)
		assert.NoError(t, err)
		assert.Equal(t, expected, actual, "URNs mismatch for contact %d", contactID)
	}

	assertContactURNs(CathyID, []string{"tel:+16055741111"})
	assertContactURNs(BobID, []string{"tel:+16055742222"})
	assertContactURNs(GeorgeID, []string{"tel:+16055743333"})

	cathyURN := urns.URN(fmt.Sprintf("tel:+16055741111?id=%d", CathyURNID))
	bobURN := urns.URN(fmt.Sprintf("tel:+16055742222?id=%d", BobURNID))

	// give Cathy a new higher priority URN
	err = UpdateContactURNs(ctx, db, oa, []*ContactURNsChanged{{CathyID, Org1, []urns.URN{"tel:+16055700001", cathyURN}}})
	assert.NoError(t, err)

	assertContactURNs(CathyID, []string{"tel:+16055700001", "tel:+16055741111"})

	// give Bob a new lower priority URN
	err = UpdateContactURNs(ctx, db, oa, []*ContactURNsChanged{{BobID, Org1, []urns.URN{bobURN, "tel:+16055700002"}}})
	assert.NoError(t, err)

	assertContactURNs(BobID, []string{"tel:+16055742222", "tel:+16055700002"})
	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM contacts_contacturn WHERE contact_id IS NULL`, nil, 0) // shouldn't be any orphan URNs
	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM contacts_contacturn`, nil, numInitialURNs+2)           // but 2 new URNs

	// remove a URN from Cathy
	err = UpdateContactURNs(ctx, db, oa, []*ContactURNsChanged{{CathyID, Org1, []urns.URN{"tel:+16055700001"}}})
	assert.NoError(t, err)

	assertContactURNs(CathyID, []string{"tel:+16055700001"})
	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM contacts_contacturn WHERE contact_id IS NULL`, nil, 1) // now orphaned

	// steal a URN from Bob
	err = UpdateContactURNs(ctx, db, oa, []*ContactURNsChanged{{CathyID, Org1, []urns.URN{"tel:+16055700001", "tel:+16055700002"}}})
	assert.NoError(t, err)

	assertContactURNs(CathyID, []string{"tel:+16055700001", "tel:+16055700002"})
	assertContactURNs(BobID, []string{"tel:+16055742222"})

	// steal the URN back from Cathy whilst simulataneously adding new URN to Cathy and not-changing anything for George
	err = UpdateContactURNs(ctx, db, oa, []*ContactURNsChanged{
		{BobID, Org1, []urns.URN{"tel:+16055742222", "tel:+16055700002"}},
		{CathyID, Org1, []urns.URN{"tel:+16055700001", "tel:+16055700003"}},
		{GeorgeID, Org1, []urns.URN{"tel:+16055743333"}},
	})
	assert.NoError(t, err)

	assertContactURNs(CathyID, []string{"tel:+16055700001", "tel:+16055700003"})
	assertContactURNs(BobID, []string{"tel:+16055742222", "tel:+16055700002"})
	assertContactURNs(GeorgeID, []string{"tel:+16055743333"})

	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM contacts_contacturn`, nil, numInitialURNs+3)
}
