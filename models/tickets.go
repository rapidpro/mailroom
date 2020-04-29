package models

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"net/http"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/services/ticket/mailgun"
	"github.com/nyaruka/goflow/services/ticket/zendesk"
	"github.com/nyaruka/goflow/utils/httpx"
	"github.com/nyaruka/goflow/utils/uuids"
	"github.com/nyaruka/mailroom/goflow"
	"github.com/nyaruka/null"
	"github.com/sirupsen/logrus"

	"github.com/pkg/errors"
)

type TicketID int
type TicketerID null.Int
type TicketStatus string

const (
	// ticketer types
	TicketerTypeMailgun = "mailgun"
	TicketerTypeZendesk = "zendesk"

	// Mailgun config options
	MailgunConfigDomain    = "domain"
	MailgunConfigAPIKey    = "api_key"
	MailgunConfigToAddress = "to_address"

	// Zendesk config options
	ZendeskConfigSubdomain = "subdomain"
	ZendeskConfigUsername  = "username"
	ZendeskConfigAPIToken  = "api_token"

	TicketStatusOpen   = TicketStatus("O")
	TicketStatusClosed = TicketStatus("C")
)

// Register a ticket service factory with the engine
func init() {
	httpClient := &http.Client{Timeout: time.Duration(15 * time.Second)}
	httpRetries := httpx.NewFixedRetries(3, 10)

	goflow.RegisterTicketServiceFactory(
		func(session flows.Session, ticketer *flows.Ticketer) (flows.TicketService, error) {
			return ticketer.Asset().(*Ticketer).AsService(httpClient, httpRetries, ticketer)
		},
	)
}

type Ticket struct {
	t struct {
		ID         TicketID         `db:"id"`
		UUID       flows.TicketUUID `db:"uuid"`
		OrgID      OrgID            `db:"org_id"`
		ContactID  ContactID        `db:"contact_id"`
		TicketerID TicketerID       `db:"ticketer_id"`
		ExternalID null.String      `db:"external_id"`
		Status     TicketStatus     `db:"status"`
		Subject    string           `db:"subject"`
		Body       string           `db:"body"`
		Config     null.Map         `db:"config"`
		OpenedOn   time.Time        `db:"opened_on"`
		ModifiedOn time.Time        `db:"modified_on"`
		ClosedOn   *time.Time       `db:"closed_on"`
	}
}

// NewTicket creates a new open ticket
func NewTicket(uuid flows.TicketUUID, orgID OrgID, contactID ContactID, ticketerID TicketerID, externalID, subject, body string) *Ticket {
	t := &Ticket{}
	t.t.UUID = uuid
	t.t.OrgID = orgID
	t.t.ContactID = contactID
	t.t.TicketerID = ticketerID
	t.t.ExternalID = null.String(externalID)
	t.t.Status = TicketStatusOpen
	t.t.Subject = subject
	t.t.Body = body
	return t
}

func (t *Ticket) ID() TicketID            { return t.t.ID }
func (t *Ticket) UUID() flows.TicketUUID  { return t.t.UUID }
func (t *Ticket) OrgID() OrgID            { return t.t.OrgID }
func (t *Ticket) ContactID() ContactID    { return t.t.ContactID }
func (t *Ticket) ExternalID() null.String { return t.t.ExternalID }
func (t *Ticket) Status() TicketStatus    { return t.t.Status }
func (t *Ticket) Config() null.Map        { return t.t.Config }

const selectOpenTicketSQL = `
SELECT
  id,
  uuid,
  org_id,
  contact_id,
  ticketer_id,
  external_id,
  status,
  subject,
  body,
  config,
  opened_on,
  modified_on,
  closed_on
FROM
  tickets_ticket
WHERE
  org_id = $1 AND
  contact_id = $2 AND
  status = 'O'
ORDER BY
  opened_on DESC
`

// LookupTicketForContact looks up the most recent open ticket for the passed in org and contact
func LookupTicketForContact(ctx context.Context, db Queryer, org *OrgAssets, contact *Contact) (*Ticket, error) {
	rows, err := db.QueryxContext(ctx, selectOpenTicketSQL, org.OrgID(), contact.ID())
	if err != nil && err != sql.ErrNoRows {
		return nil, errors.Wrapf(err, "error querying for open ticket for contact: %d", contact.ID())
	}
	defer rows.Close()

	if err == sql.ErrNoRows || !rows.Next() {
		return nil, nil
	}

	ticket := &Ticket{}
	err = rows.StructScan(&ticket.t)
	if err != nil {
		return nil, errors.Wrapf(err, "error reading open ticket for contact: %d", contact.ID())
	}

	return ticket, nil
}

const selectTicketSQL = `
SELECT
  id,
  uuid,
  org_id,
  contact_id,
  ticketer_id,
  external_id,
  status,
  subject,
  body,
  config,
  opened_on,
  modified_on,
  closed_on
FROM
  tickets_ticket
WHERE
  uuid = $1
`

// LookupTicketByUUID looks up the ticket with the passed in UUID
func LookupTicketByUUID(ctx context.Context, db Queryer, uuid uuids.UUID) (*Ticket, error) {
	rows, err := db.QueryxContext(ctx, selectTicketSQL, string(uuid))
	if err != nil && err != sql.ErrNoRows {
		return nil, errors.Wrapf(err, "error querying for ticket for uuid: %s", string(uuid))
	}
	defer rows.Close()

	if err == sql.ErrNoRows || !rows.Next() {
		return nil, nil
	}

	ticket := &Ticket{}
	err = rows.StructScan(&ticket.t)
	if err != nil {
		return nil, errors.Wrapf(err, "error reading ticket for uuid: %s", string(uuid))
	}

	return ticket, nil
}

const insertOpenTicketSQL = `
INSERT INTO 
  tickets_ticket(uuid,  org_id,  contact_id,  ticketer_id,  external_id,  status,  subject,  body,  config,  opened_on,  modified_on)
  VALUES(        :uuid, :org_id, :contact_id, :ticketer_id, :external_id, :status, :subject, :body, :config, NOW(),      NOW()      )
RETURNING
  id
`

// InsertTickets inserts the passed in tickets returning any errors encountered
func InsertTickets(ctx context.Context, tx Queryer, tickets []*Ticket) error {
	if len(tickets) == 0 {
		return nil
	}

	ts := make([]interface{}, len(tickets))
	for i := range tickets {
		ts[i] = &tickets[i].t
	}

	return BulkSQL(ctx, "inserted tickets", tx, insertOpenTicketSQL, ts)
}

const updateTicketSQL = `
UPDATE
  tickets_ticket
SET
  status = $3,
  config = $4
WHERE
  id = $1
`

// UpdateTicket updates the passed in ticket with the passed in external id, status and config
func UpdateTicket(ctx context.Context, db Queryer, ticket *Ticket, status TicketStatus, config null.Map) error {
	t := &ticket.t
	t.Config = config
	t.Status = status

	return Exec(ctx, "update ticket", db, updateTicketSQL, t.ID, t.Status, t.Config)
}

const closeTicketsForContactsSQL = `
UPDATE
  tickets_ticket
SET
  status = 'C',
  modified_on = NOW(),
  closed_on = NOW()
WHERE
  contact_id = ANY($1) AND
  status = 'O'
`

// CloseTicketsForContacts closes any open tikcets for the given contacts
func CloseTicketsForContacts(ctx context.Context, db Queryer, contactIDs []ContactID) error {
	err := Exec(ctx, "close ticket", db, closeTicketsForContactsSQL, pq.Array(contactIDs))
	if err != nil {
		return errors.Wrapf(err, "error closing tickets for %d contacts", len(contactIDs))
	}

	return nil
}

// Ticketer is our type for a ticketer asset
type Ticketer struct {
	t struct {
		ID     TicketerID          `json:"id"`
		UUID   assets.TicketerUUID `json:"uuid"`
		Type   string              `json:"ticketer_type"`
		Name   string              `json:"name"`
		Config map[string]string   `json:"config"`
	}
}

// ID returns the ID
func (t *Ticketer) ID() TicketerID { return t.t.ID }

// UUID returns the UUID
func (t *Ticketer) UUID() assets.TicketerUUID { return t.t.UUID }

// Name returns the name
func (t *Ticketer) Name() string { return t.t.Name }

// Type returns the type
func (t *Ticketer) Type() string { return t.t.Type }

// AsService builds the corresponding engine service for the passed in Ticketer
func (t *Ticketer) AsService(httpClient *http.Client, httpRetries *httpx.RetryConfig, ticketer *flows.Ticketer) (flows.TicketService, error) {
	switch t.Type() {
	case TicketerTypeMailgun:
		domain := t.t.Config[MailgunConfigDomain]
		apiKey := t.t.Config[MailgunConfigAPIKey]
		toAddress := t.t.Config[MailgunConfigToAddress]
		if domain != "" && apiKey != "" && toAddress != "" {
			return mailgun.NewService(httpClient, httpRetries, ticketer, domain, apiKey, toAddress), nil
		}
		return nil, errors.New("missing domain or api_key or to_address in mailgun config")
	case TicketerTypeZendesk:
		subdomain := t.t.Config[ZendeskConfigSubdomain]
		username := t.t.Config[ZendeskConfigUsername]
		apiToken := t.t.Config[ZendeskConfigAPIToken]
		if subdomain != "" && username != "" && apiToken != "" {
			return zendesk.NewService(httpClient, httpRetries, ticketer, subdomain, username, apiToken), nil
		}
		return nil, errors.New("missing subdomain or username or api_token in zendesk config")
	}
	return nil, errors.Errorf("unrecognized ticket service type '%s'", t.Type())
}

const selectTicketersSQL = `
SELECT ROW_TO_JSON(r) FROM (SELECT
	t.id as id,
	t.uuid as uuid,
	t.name as name,
	t.ticketer_type as ticketer_type,
	t.config as config
FROM 
	tickets_ticketer t
WHERE 
	t.org_id = $1 AND 
	t.is_active = TRUE
ORDER BY
	t.created_on ASC
) r;
`

// loadTicketers loads all the ticketers for the passed in org
func loadTicketers(ctx context.Context, db sqlx.Queryer, orgID OrgID) ([]assets.Ticketer, error) {
	start := time.Now()

	rows, err := db.Queryx(selectTicketersSQL, orgID)
	if err != nil {
		return nil, errors.Wrapf(err, "error querying ticketers for org: %d", orgID)
	}
	defer rows.Close()

	ticketers := make([]assets.Ticketer, 0, 2)
	for rows.Next() {
		ticketer := &Ticketer{}
		err := readJSONRow(rows, &ticketer.t)
		if err != nil {
			return nil, errors.Wrapf(err, "error unmarshalling ticketer")
		}
		ticketers = append(ticketers, ticketer)
	}

	logrus.WithField("elapsed", time.Since(start)).WithField("org_id", orgID).WithField("count", len(ticketers)).Debug("loaded ticketers")

	return ticketers, nil
}

// MarshalJSON marshals into JSON. 0 values will become null
func (i TicketerID) MarshalJSON() ([]byte, error) {
	return null.Int(i).MarshalJSON()
}

// UnmarshalJSON unmarshals from JSON. null values become 0
func (i *TicketerID) UnmarshalJSON(b []byte) error {
	return null.UnmarshalInt(b, (*null.Int)(i))
}

// Value returns the db value, null is returned for 0
func (i TicketerID) Value() (driver.Value, error) {
	return null.Int(i).Value()
}

// Scan scans from the db value. null values become 0
func (i *TicketerID) Scan(value interface{}) error {
	return null.ScanInt(value, (*null.Int)(i))
}
