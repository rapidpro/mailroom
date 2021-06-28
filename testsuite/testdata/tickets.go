package testdata

import (
	"testing"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/core/models"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

// InsertOpenTicket inserts an open ticket
func InsertOpenTicket(t *testing.T, db *sqlx.DB, orgID models.OrgID, contactID models.ContactID, ticketerID models.TicketerID, uuid flows.TicketUUID, subject, body, externalID string) models.TicketID {
	var id models.TicketID
	err := db.Get(&id,
		`INSERT INTO tickets_ticket(uuid, org_id, contact_id, ticketer_id, status, subject, body, external_id, opened_on, modified_on)
		VALUES($1, $2, $3, $4, 'O', $5, $6, $7, NOW(), NOW()) RETURNING id`, uuid, orgID, contactID, ticketerID, subject, body, externalID,
	)
	require.NoError(t, err)
	return id
}

// InsertClosedTicket inserts a closed ticket
func InsertClosedTicket(t *testing.T, db *sqlx.DB, orgID models.OrgID, contactID models.ContactID, ticketerID models.TicketerID, uuid flows.TicketUUID, subject, body, externalID string) models.TicketID {
	var id models.TicketID
	err := db.Get(&id,
		`INSERT INTO tickets_ticket(uuid, org_id, contact_id, ticketer_id, status, subject, body, external_id, opened_on, modified_on, closed_on)
		VALUES($1, $2, $3, $4, 'C', $5, $6, $7, NOW(), NOW(), NOW()) RETURNING id`, uuid, orgID, contactID, ticketerID, subject, body, externalID,
	)
	require.NoError(t, err)
	return id
}
