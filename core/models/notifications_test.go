package models_test

import (
	"context"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTicketOpenedNotifications(t *testing.T) {
	ctx, _, db, _ := testsuite.Get()

	oa, err := models.GetOrgAssets(ctx, db, testdata.Org1.ID)
	require.NoError(t, err)

	// open an unassigned ticket by a flow (i.e. no user)
	_, openedEvent := openTicket(t, ctx, db, nil, nil)
	err = models.LogTicketsOpened(ctx, db, oa, []*models.TicketEvent{openedEvent})
	require.NoError(t, err)

	// check that all assignable users are notified
	log := assertNotifications(t, ctx, db, models.LogTypeTicketOpened, nil, []*testdata.User{testdata.Admin, testdata.Editor, testdata.Agent})
	assert.Equal(t, openedEvent.TicketID(), log.TicketID)
	assert.Equal(t, openedEvent.ID(), log.TicketEventID)

	// open an unassigned ticket by a user
	_, openedEvent = openTicket(t, ctx, db, testdata.Editor, nil)
	err = models.LogTicketsOpened(ctx, db, oa, []*models.TicketEvent{openedEvent})
	require.NoError(t, err)

	// check that all assignable users are notified except the user who opened the ticket
	assertNotifications(t, ctx, db, models.LogTypeTicketOpened, testdata.Editor, []*testdata.User{testdata.Admin, testdata.Agent})

	// open an already assigned ticket
	_, openedEvent = openTicket(t, ctx, db, nil, testdata.Agent)
	err = models.LogTicketsOpened(ctx, db, oa, []*models.TicketEvent{openedEvent})
	require.NoError(t, err)

	// check that the log type is actually assigned and that we notify the assigned user only
	assertNotifications(t, ctx, db, models.LogTypeTicketAssigned, nil, []*testdata.User{testdata.Agent})

	// unless they self-assigned..
	_, openedEvent = openTicket(t, ctx, db, testdata.Agent, testdata.Agent)
	err = models.LogTicketsOpened(ctx, db, oa, []*models.TicketEvent{openedEvent})
	require.NoError(t, err)

	// in which case there is nobody to notify
	assertNotifications(t, ctx, db, models.LogTypeTicketAssigned, testdata.Agent, nil)
}

func TestTicketAssignedNotifications(t *testing.T) {
	ctx, _, db, _ := testsuite.Get()

	oa, err := models.GetOrgAssets(ctx, db, testdata.Org1.ID)
	require.NoError(t, err)

	// open an unassigned ticket
	ticket, _ := openTicket(t, ctx, db, nil, nil)
	require.NoError(t, err)

	assignedEvent := models.NewTicketAssignedEvent(ticket, testdata.Admin.ID, testdata.Agent.ID, "please")
	err = models.InsertTicketEvents(ctx, db, []*models.TicketEvent{assignedEvent})
	require.NoError(t, err)

	err = models.LogTicketsAssigned(ctx, db, oa, []*models.TicketEvent{assignedEvent})
	require.NoError(t, err)

	// check that assignee is notified
	assertNotifications(t, ctx, db, models.LogTypeTicketAssigned, testdata.Admin, []*testdata.User{testdata.Agent})
}

func assertNotifications(t *testing.T, ctx context.Context, db *sqlx.DB, expectedType models.LogType, expectedCreatedBy *testdata.User, expectedNotified []*testdata.User) *models.Log {
	// check last log
	log := &models.Log{}
	err := db.GetContext(ctx, log, `SELECT id, org_id, log_type, created_by_id, ticket_id, ticket_event_id FROM notifications_log ORDER BY id DESC LIMIT 1`)
	require.NoError(t, err)

	assert.Equal(t, expectedType, log.LogType, "log type mismatch")
	assert.Equal(t, expectedCreatedBy.SafeID(), log.CreatedByID, "log created by mismatch")

	// check who was notified
	var actualNotifiedIDs []models.UserID
	err = db.SelectContext(ctx, &actualNotifiedIDs, `SELECT user_id FROM notifications_notification WHERE log_id = $1`, log.ID)
	require.NoError(t, err)

	expectedNotifiedIDs := make([]models.UserID, len(expectedNotified))
	for i := range expectedNotified {
		expectedNotifiedIDs[i] = expectedNotified[i].ID
	}

	assert.ElementsMatch(t, expectedNotifiedIDs, actualNotifiedIDs, "notified users mismatch")

	return log
}

func openTicket(t *testing.T, ctx context.Context, db *sqlx.DB, openedBy *testdata.User, assignee *testdata.User) (*models.Ticket, *models.TicketEvent) {
	ticket := testdata.InsertOpenTicket(db, testdata.Org1, testdata.Cathy, testdata.Internal, testdata.SupportTopic, "", "Where my pants", "", assignee)
	modelTicket := ticket.Load(db)

	openedEvent := models.NewTicketOpenedEvent(modelTicket, openedBy.SafeID(), assignee.SafeID())
	err := models.InsertTicketEvents(ctx, db, []*models.TicketEvent{openedEvent})
	require.NoError(t, err)

	return modelTicket, openedEvent
}
