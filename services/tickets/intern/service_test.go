package intern_test

import (
	"net/http"
	"testing"

	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/assets/static"
	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/test"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/core/models"
	intern "github.com/nyaruka/mailroom/services/tickets/intern"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpenAndForward(t *testing.T) {
	ctx, rt, _, _ := testsuite.Get()

	session, _, err := test.CreateTestSession("", envs.RedactionPolicyNone)
	require.NoError(t, err)

	defer uuids.SetGenerator(uuids.DefaultGenerator)
	uuids.SetGenerator(uuids.NewSeededGenerator(12345))

	ticketer := flows.NewTicketer(static.NewTicketer(assets.TicketerUUID(uuids.New()), "Support", "internal"))

	svc, err := intern.NewService(
		rt.Config,
		http.DefaultClient,
		nil,
		ticketer,
		nil,
	)
	require.NoError(t, err)

	logger := &flows.HTTPLogger{}

	oa, err := models.GetOrgAssets(ctx, rt, testdata.Org1.ID)
	require.NoError(t, err)
	defaultTopic := oa.SessionAssets().Topics().FindByName("General")

	ticket, err := svc.Open(session, defaultTopic, "Where are my cookies?", nil, logger.Log)
	assert.NoError(t, err)
	assert.Equal(t, flows.TicketUUID("e7187099-7d38-4f60-955c-325957214c42"), ticket.UUID())
	assert.Equal(t, "General", ticket.Topic().Name())
	assert.Equal(t, "Where are my cookies?", ticket.Body())
	assert.Equal(t, "", ticket.ExternalID())
	assert.Equal(t, 0, len(logger.Logs))

	dbTicket := models.NewTicket(ticket.UUID(), testdata.Org1.ID, testdata.Cathy.ID, testdata.Internal.ID, "", testdata.DefaultTopic.ID, "Where are my cookies?", models.NilUserID, nil)

	logger = &flows.HTTPLogger{}
	err = svc.Forward(
		dbTicket,
		flows.MsgUUID("ca5607f0-cba8-4c94-9cd5-c4fbc24aa767"),
		"It's urgent",
		[]utils.Attachment{utils.Attachment("image/jpg:http://myfiles.com/media/0123/attachment1.jpg")},
		logger.Log,
	)

	// forwarding is a NOOP for internal ticketers
	assert.NoError(t, err)
	assert.Equal(t, 0, len(logger.Logs))
}

func TestCloseAndReopen(t *testing.T) {
	_, rt, _, _ := testsuite.Get()

	defer uuids.SetGenerator(uuids.DefaultGenerator)
	uuids.SetGenerator(uuids.NewSeededGenerator(12345))

	ticketer := flows.NewTicketer(static.NewTicketer(assets.TicketerUUID(uuids.New()), "Support", "internal"))
	svc, err := intern.NewService(rt.Config, http.DefaultClient, nil, ticketer, nil)
	require.NoError(t, err)

	logger := &flows.HTTPLogger{}
	ticket1 := models.NewTicket("88bfa1dc-be33-45c2-b469-294ecb0eba90", testdata.Org1.ID, testdata.Cathy.ID, testdata.Internal.ID, "12", testdata.DefaultTopic.ID, "Where my cookies?", models.NilUserID, nil)
	ticket2 := models.NewTicket("645eee60-7e84-4a9e-ade3-4fce01ae28f1", testdata.Org1.ID, testdata.Bob.ID, testdata.Internal.ID, "14", testdata.DefaultTopic.ID, "Where my shoes?", models.NilUserID, nil)

	err = svc.Close([]*models.Ticket{ticket1, ticket2}, logger.Log)

	// NOOP
	assert.NoError(t, err)
	assert.Equal(t, 0, len(logger.Logs))

	err = svc.Reopen([]*models.Ticket{ticket2}, logger.Log)

	// NOOP
	assert.NoError(t, err)
	assert.Equal(t, 0, len(logger.Logs))
}
