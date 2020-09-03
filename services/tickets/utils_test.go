package tickets_test

import (
	"io/ioutil"
	"testing"

	"github.com/nyaruka/gocommon/httpx"
	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/services/tickets"
	_ "github.com/nyaruka/mailroom/services/tickets/mailgun"
	_ "github.com/nyaruka/mailroom/services/tickets/zendesk"
	"github.com/nyaruka/mailroom/testsuite"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetContactDisplay(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()

	oa, err := models.GetOrgAssets(ctx, db, models.Org1)
	require.NoError(t, err)

	contact, err := models.LoadContact(ctx, db, oa, models.CathyID)
	require.NoError(t, err)

	flowContact, err := contact.FlowContact(oa)
	require.NoError(t, err)

	// name if they have one
	assert.Equal(t, "Cathy", tickets.GetContactDisplay(oa.Env(), flowContact))

	flowContact.SetName("")

	// or primary URN
	assert.Equal(t, "(605) 574-1111", tickets.GetContactDisplay(oa.Env(), flowContact))

	// but not if org is anon
	anonEnv := envs.NewBuilder().WithRedactionPolicy(envs.RedactionPolicyURNs).Build()
	assert.Equal(t, "10000", tickets.GetContactDisplay(anonEnv, flowContact))
}

func TestFromTicketUUID(t *testing.T) {
	testsuite.ResetDB()
	ctx := testsuite.CTX()
	db := testsuite.DB()

	ticket1UUID := flows.TicketUUID("f7358870-c3dd-450d-b5ae-db2eb50216ba")
	ticket2UUID := flows.TicketUUID("44b7d9b5-6ddd-4a6a-a1c0-8b70ecd06339")

	// create some tickets
	db.MustExec(`INSERT INTO tickets_ticket(id, uuid,  org_id, contact_id, ticketer_id, status, subject, body, opened_on, modified_on)
	VALUES(1, $1, $2, $3, $4, 'O', 'Need help', 'Have you seen my cookies?', NOW(), NOW())`, ticket1UUID, models.Org1, models.CathyID, models.MailgunID)

	db.MustExec(`INSERT INTO tickets_ticket(id, uuid,  org_id, contact_id, ticketer_id, status, subject, body, opened_on, modified_on)
	VALUES(2, $1, $2, $3, $4, 'O', 'Need help', 'Have you seen my shoes?', NOW(), NOW())`, ticket2UUID, models.Org1, models.CathyID, models.ZendeskID)

	// break mailgun configuration
	db.MustExec(`UPDATE tickets_ticketer SET config = '{"foo":"bar"}'::jsonb WHERE id = $1`, models.MailgunID)

	models.FlushCache()

	// err if no ticket with UUID
	_, _, _, err := tickets.FromTicketUUID(ctx, db, "33c54d0c-bd49-4edf-87a9-c391a75a630c", "mailgun")
	assert.EqualError(t, err, "error looking up ticket 33c54d0c-bd49-4edf-87a9-c391a75a630c")

	// err if no ticketer type doesn't match
	_, _, _, err = tickets.FromTicketUUID(ctx, db, ticket1UUID, "zendesk")
	assert.EqualError(t, err, "error looking up ticketer #1")

	// err if ticketer isn't configured correctly and can't be loaded as a service
	_, _, _, err = tickets.FromTicketUUID(ctx, db, ticket1UUID, "mailgun")
	assert.EqualError(t, err, "error loading ticketer service: missing domain or api_key or to_address or url_base in mailgun config")

	// if all is correct, returns the ticket, ticketer asset, and ticket service
	ticket, ticketer, svc, err := tickets.FromTicketUUID(ctx, db, ticket2UUID, "zendesk")

	assert.Equal(t, ticket2UUID, ticket.UUID())
	assert.Equal(t, models.ZendeskUUID, ticketer.UUID())
	assert.Implements(t, (*models.TicketService)(nil), svc)

	testsuite.ResetDB()
	models.FlushCache()
}

func TestFromTicketerUUID(t *testing.T) {
	testsuite.ResetDB()
	ctx := testsuite.CTX()
	db := testsuite.DB()

	// break mailgun configuration
	db.MustExec(`UPDATE tickets_ticketer SET config = '{"foo":"bar"}'::jsonb WHERE id = $1`, models.MailgunID)

	// err if no ticketer with UUID
	_, _, err := tickets.FromTicketerUUID(ctx, db, "33c54d0c-bd49-4edf-87a9-c391a75a630c", "mailgun")
	assert.EqualError(t, err, "error looking up ticketer 33c54d0c-bd49-4edf-87a9-c391a75a630c")

	// err if no ticketer type doesn't match
	_, _, err = tickets.FromTicketerUUID(ctx, db, models.MailgunUUID, "zendesk")
	assert.EqualError(t, err, "error looking up ticketer f9c9447f-a291-4f3c-8c79-c089bbd4e713")

	// err if ticketer isn't configured correctly and can't be loaded as a service
	_, _, err = tickets.FromTicketerUUID(ctx, db, models.MailgunUUID, "mailgun")
	assert.EqualError(t, err, "error loading ticketer service: missing domain or api_key or to_address or url_base in mailgun config")

	// if all is correct, returns the ticketer asset and ticket service
	ticketer, svc, err := tickets.FromTicketerUUID(ctx, db, models.ZendeskUUID, "zendesk")

	assert.Equal(t, models.ZendeskUUID, ticketer.UUID())
	assert.Implements(t, (*models.TicketService)(nil), svc)

	testsuite.ResetDB()
	models.FlushCache()
}

func TestSendReply(t *testing.T) {
	testsuite.ResetDB()
	ctx := testsuite.CTX()
	db := testsuite.DB()
	rp := testsuite.RP()
	defer testsuite.ResetStorage()

	defer uuids.SetGenerator(uuids.DefaultGenerator)
	uuids.SetGenerator(uuids.NewSeededGenerator(12345))

	image, err := ioutil.ReadFile("../../models/testdata/test.jpg")
	require.NoError(t, err)

	defer httpx.SetRequestor(httpx.DefaultRequestor)
	httpx.SetRequestor(httpx.NewMockRequestor(map[string][]httpx.MockResponse{
		"http://coolfilesfortickets.com/a.jpg": {httpx.MockResponse{Status: 200, Body: image}},
		"http://badfiles.com/b.jpg":            {httpx.MockResponse{Status: 400, Body: nil}},
	}))

	ticketUUID := flows.TicketUUID("f7358870-c3dd-450d-b5ae-db2eb50216ba")

	// create a ticket
	db.MustExec(`INSERT INTO tickets_ticket(id, uuid,  org_id, contact_id, ticketer_id, status, subject, body, opened_on, modified_on)
	VALUES(1, $1, $2, $3, $4, 'O', 'Need help', 'Have you seen my cookies?', NOW(), NOW())`, ticketUUID, models.Org1, models.CathyID, models.MailgunID)

	ticket, err := models.LookupTicketByUUID(ctx, db, ticketUUID)
	require.NoError(t, err)

	msg, err := tickets.SendReply(ctx, db, rp, testsuite.Storage(), ticket, "I'll get back to you", []string{"http://coolfilesfortickets.com/a.jpg"})
	require.NoError(t, err)

	assert.Equal(t, "I'll get back to you", msg.Text())
	assert.Equal(t, models.CathyID, msg.ContactID())
	assert.Equal(t, []utils.Attachment{"image/jpeg:https:///_test_storage/media/1/1ae9/6956/1ae96956-4b34-433e-8d1a-f05fe6923d6d.jpg"}, msg.Attachments())
	assert.FileExists(t, "_test_storage/media/1/1ae9/6956/1ae96956-4b34-433e-8d1a-f05fe6923d6d.jpg")

	// try with file that can't be fetched
	_, err = tickets.SendReply(ctx, db, rp, testsuite.Storage(), ticket, "I'll get back to you", []string{"http://badfiles.com/b.jpg"})
	assert.EqualError(t, err, "error fetching file http://badfiles.com/b.jpg for ticket reply: fetch returned non-200 response")
}
