package models

import (
	"context"
	"database/sql"
	"time"

	"github.com/nyaruka/goflow/utils/uuids"
	"github.com/nyaruka/null"
	"github.com/pkg/errors"
)

type TicketID int
type ServiceID int

type Ticket struct {
	t struct {
		ID         TicketID    `db:"id"`
		UUID       uuids.UUID  `db:"uuid"`
		OrgID      OrgID       `db:"org_id"`
		ServiceID  ServiceID   `db:"service_id"`
		ContactID  ContactID   `db:"contact_id"`
		ExternalID null.String `db:"external_id"`
		Subject    string      `db:"subject"`
		Config     null.Map    `db:"config"`
		Status     string      `db:"status"`
		OpenedOn   time.Time   `db:"opened_on"`
		ModifiedOn time.Time   `db:"modified_on"`
		ClosedOn   *time.Time  `db:"closed_on"`
	}
}

func (t *Ticket) ContactID() ContactID    { return t.t.ContactID }
func (t *Ticket) OrgID() OrgID            { return t.t.OrgID }
func (t *Ticket) ID() uuids.UUID          { return t.t.UUID }
func (t *Ticket) UUID() uuids.UUID        { return t.t.UUID }
func (t *Ticket) ExternalID() null.String { return t.t.ExternalID }
func (t *Ticket) Status() string          { return t.t.Status }
func (t *Ticket) Config() null.Map        { return t.t.Config }

const selectActiveTicketSQL = `
SELECT
  id,
  service_id,
  uuid,
  external_id,
  status,
  subject,
  config,
  opened_on,
  ended_on,
  contact_id,
  org_id
FROM
  threads_thread
WHERE
  org_id = $1 AND
  contact_id = $2 AND
  status = 'O'
ORDER BY
  created_on DESC
`

// LookupTicketForContact looks up the most recent open ticket for the passed in org and contact
func LookupTicketForContact(ctx context.Context, db Queryer, org *OrgAssets, contact *Contact) (*Ticket, error) {
	rows, err := db.QueryxContext(ctx, selectActiveTicketSQL, org.OrgID(), contact.ID())
	if err != nil && err != sql.ErrNoRows {
		return nil, errors.Wrapf(err, "error querying for active ticket for contact: %d", contact.ID())
	}
	defer rows.Close()

	if err == sql.ErrNoRows || !rows.Next() {
		return nil, nil
	}

	ticket := &Ticket{}
	err = rows.StructScan(&ticket.t)
	if err != nil {
		return nil, errors.Wrapf(err, "error reading active ticket for contact: %d", contact.ID())
	}

	return ticket, nil
}

const selectTicketSQL = `
SELECT
  id,
  service_id,
  uuid,
  external_id,
  subject,
  status,
  config,
  opened_on,
  closed_on,
  contact_id,
  org_id
FROM
  threads_thread
WHERE
  uuid = $1
`

// LookupTicketByUUID looks up the ticket with the passed in UUID
func LookupTicketByUUID(ctx context.Context, db Queryer, uuid uuids.UUID) (*Ticket, error) {
	rows, err := db.QueryxContext(ctx, selectTicketSQL, string(uuid))
	if err != nil && err != sql.ErrNoRows {
		return nil, errors.Wrapf(err, "error querying for ticket for uuid: %d", uuid)
	}
	defer rows.Close()

	if err == sql.ErrNoRows || !rows.Next() {
		return nil, nil
	}

	ticket := &Ticket{}
	err = rows.StructScan(&ticket.t)
	if err != nil {
		return nil, errors.Wrapf(err, "error reading ticket for uuid: %d", uuid)
	}

	return ticket, nil
}

const insertTicketSQL = `
INSERT INTO 
  tickets_ticket(uuid, org_id, service_id, contact_id, status, subject, opened_on)
  VALUES(:uuid, :org_id, :service_id, :contact_id, :status, :subject, :opened_on)
RETURNING
  id
`

// TODO: this needs to be passed in a service which does the actual forwarding / opening side of things
// InsertAndForwardTicket opens a new ticket for the passed in contact and service, forwarding it onto the service after
// insertion.
func InsertAndForwardTicket(ctx context.Context, db Queryer, org *OrgAssets, contact *Contact, ticket *Ticket, msg string) error {
	// by default we send to our org ticket email
	email := org.Org().ConfigValue("thread_email", "")
	if email == "" {
		return errors.Errorf("Org %d has no thread email, cannot forward", org.OrgID())
	}

	// if we don't have a thread, create one
	if ticket == nil {
		ticket = &Ticket{}
		t := &ticket.t
		t.UUID = uuids.New()
		t.OpenedOn = time.Now()
		t.ContactID = contact.ID()
		t.OrgID = org.OrgID()
		t.Status = "O"

		err := BulkSQL(ctx, "insert new thread", db, insertTicketSQL, []interface{}{&ticket.t})
		if err != nil {
			return errors.Wrapf(err, "error inserting new ticket")
		}
	}

	// everything here should be moved to the EmailTicketingService
	/*
		// if we have a ticket email, use that
		if ticket.ExternalID() != "" {
			email = string(thread.ExternalID())
		}

		subject := thread.t.Config.GetString("subject", msg)
		body := thread.t.Config.GetString("body", "")
		lastMessageID := thread.t.Config.GetString("last-message-id", "")

		// forward our message appropriately
		from := "thread+" + string(thread.UUID()) + "@mr.nyaruka.com"

		// create our dialer for our org
		// TODO: everything below should be coming from environment
		d := mail.NewDialer("smtp.mailgun.org", 465, "postmaster@mr.nyaruka.com", "smtp-password-from-env")

		m := mail.NewMessage()
		m.SetHeader("From", contact.Name()+"<"+from+">")
		m.SetHeader("To", email)
		m.SetHeader("Subject", subject)
		m.SetHeader("In-Reply-To", lastMessageID)
		m.SetBody("text/plain", msg+body)
		err := d.DialAndSend(m)
		if err != nil {
			return errors.Wrapf(err, "error forwarding message")
		}

		logrus.WithFields(logrus.Fields{
			"thread_id": thread.ID(),
			"from":      from,
			"to":        email,
			"message":   msg + body,
			"subject":   subject,
			"reply-to":  lastMessageID,
		}).Info("message forwarded")
	*/

	return nil
}

const updateTicketSQL = `
UPDATE
  tickets_ticket
SET
  external_id = $2,
  status = $3,
  config = $4
WHERE
  id = $1
`

// UpdateTicket updates the passed in ticket with the passed in external id, status and config
func UpdateTicket(ctx context.Context, db Queryer, ticket *Ticket, externalID null.String, status string, config null.Map) error {
	t := &ticket.t
	t.ExternalID = null.String(externalID)
	t.Config = config
	t.Status = status

	err := Exec(ctx, "update ticket", db, updateTicketSQL, t.ID, t.ExternalID, t.Status, t.Config)
	if err != nil {
		return errors.Wrapf(err, "error updating ticket: %s", t.UUID)
	}

	return nil
}
