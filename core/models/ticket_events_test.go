package models_test

import (
	"testing"
	"time"

	"github.com/nyaruka/gocommon/dbutil/assertdb"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/nyaruka/null"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTicketEvents(t *testing.T) {
	ctx, _, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetData)

	ticket := testdata.InsertOpenTicket(db, testdata.Org1, testdata.Cathy, testdata.Mailgun, testdata.DefaultTopic, "Have you seen my cookies?", "17", time.Now(), nil)
	modelTicket := ticket.Load(db)

	e1 := models.NewTicketOpenedEvent(modelTicket, testdata.Admin.ID, testdata.Agent.ID)
	assert.Equal(t, testdata.Org1.ID, e1.OrgID())
	assert.Equal(t, testdata.Cathy.ID, e1.ContactID())
	assert.Equal(t, ticket.ID, e1.TicketID())
	assert.Equal(t, models.TicketEventTypeOpened, e1.EventType())
	assert.Equal(t, null.NullString, e1.Note())
	assert.Equal(t, testdata.Admin.ID, e1.CreatedByID())

	e2 := models.NewTicketAssignedEvent(modelTicket, testdata.Admin.ID, testdata.Agent.ID, "please handle")
	assert.Equal(t, models.TicketEventTypeAssigned, e2.EventType())
	assert.Equal(t, testdata.Agent.ID, e2.AssigneeID())
	assert.Equal(t, null.String("please handle"), e2.Note())
	assert.Equal(t, testdata.Admin.ID, e2.CreatedByID())

	e3 := models.NewTicketNoteAddedEvent(modelTicket, testdata.Agent.ID, "please handle")
	assert.Equal(t, models.TicketEventTypeNoteAdded, e3.EventType())
	assert.Equal(t, null.String("please handle"), e3.Note())
	assert.Equal(t, testdata.Agent.ID, e3.CreatedByID())

	e4 := models.NewTicketClosedEvent(modelTicket, testdata.Agent.ID)
	assert.Equal(t, models.TicketEventTypeClosed, e4.EventType())
	assert.Equal(t, testdata.Agent.ID, e4.CreatedByID())

	e5 := models.NewTicketReopenedEvent(modelTicket, testdata.Editor.ID)
	assert.Equal(t, models.TicketEventTypeReopened, e5.EventType())
	assert.Equal(t, testdata.Editor.ID, e5.CreatedByID())

	e6 := models.NewTicketTopicChangedEvent(modelTicket, testdata.Agent.ID, testdata.SupportTopic.ID)
	assert.Equal(t, models.TicketEventTypeTopicChanged, e6.EventType())
	assert.Equal(t, testdata.SupportTopic.ID, e6.TopicID())
	assert.Equal(t, testdata.Agent.ID, e6.CreatedByID())

	err := models.InsertTicketEvents(ctx, db, []*models.TicketEvent{e1, e2, e3, e4, e5})
	require.NoError(t, err)

	assertdb.Query(t, db, `SELECT count(*) FROM tickets_ticketevent`).Returns(5)
	assertdb.Query(t, db, `SELECT assignee_id, note FROM tickets_ticketevent WHERE id = $1`, e2.ID()).
		Columns(map[string]interface{}{"assignee_id": int64(testdata.Agent.ID), "note": "please handle"})
}
