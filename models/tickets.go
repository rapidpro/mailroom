package models

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"net/http"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/goflow/utils/httpx"
	"github.com/nyaruka/mailroom/goflow"
	"github.com/nyaruka/null"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type TicketID int
type TicketerID null.Int
type TicketStatus string

const (
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
func NewTicket(uuid flows.TicketUUID, orgID OrgID, contactID ContactID, ticketerID TicketerID, externalID, subject, body string, config map[string]interface{}) *Ticket {
	t := &Ticket{}
	t.t.UUID = uuid
	t.t.OrgID = orgID
	t.t.ContactID = contactID
	t.t.TicketerID = ticketerID
	t.t.ExternalID = null.String(externalID)
	t.t.Status = TicketStatusOpen
	t.t.Subject = subject
	t.t.Body = body
	t.t.Config = null.NewMap(config)
	return t
}

func (t *Ticket) ID() TicketID            { return t.t.ID }
func (t *Ticket) UUID() flows.TicketUUID  { return t.t.UUID }
func (t *Ticket) OrgID() OrgID            { return t.t.OrgID }
func (t *Ticket) ContactID() ContactID    { return t.t.ContactID }
func (t *Ticket) ExternalID() null.String { return t.t.ExternalID }
func (t *Ticket) Status() TicketStatus    { return t.t.Status }
func (t *Ticket) Subject() string         { return t.t.Subject }
func (t *Ticket) Config() null.Map        { return t.t.Config }

// CreateReply creates an outgoing reply in this ticket
func (t *Ticket) CreateReply(ctx context.Context, db *sqlx.DB, rp *redis.Pool, text string) (*Msg, error) {
	// look up our assets
	assets, err := GetOrgAssets(ctx, db, t.OrgID())
	if err != nil {
		return nil, errors.Wrapf(err, "error looking up org: %d", t.OrgID())
	}

	// build a simple translation
	translations := map[envs.Language]*BroadcastTranslation{
		envs.Language("base"): {Text: text},
	}

	// we'll use a broadcast to send this message
	bcast := NewBroadcast(assets.OrgID(), NilBroadcastID, translations, TemplateStateEvaluated, envs.Language("base"), nil, nil, nil)
	batch := bcast.CreateBatch([]ContactID{t.ContactID()})
	msgs, err := CreateBroadcastMessages(ctx, db, rp, assets, batch)
	if err != nil {
		return nil, errors.Wrapf(err, "error creating message batch")
	}

	return msgs[0], nil
}

// ForwardIncoming forwards an incoming message from a contact to this ticket
func (t *Ticket) ForwardIncoming(ctx context.Context, db *sqlx.DB, org *OrgAssets, contact *Contact, text string, attachments []utils.Attachment) error {
	ticketer := org.TicketerByID(t.t.TicketerID)
	if ticketer == nil {
		return errors.Errorf("can't find ticketer with id %d", t.t.TicketerID)
	}

	flowTicketer := flows.NewTicketer(ticketer)
	service, err := ticketer.AsService(http.DefaultClient, nil, flowTicketer)
	if err != nil {
		return err
	}

	logHTTP := &flows.HTTPLogger{}

	err = service.Forward(t, text, logHTTP.Log)

	// create a log for each HTTP call
	httpLogs := make([]*HTTPLog, 0, len(logHTTP.Logs))
	for _, httpLog := range logHTTP.Logs {
		httpLogs = append(httpLogs, NewTicketerCalledLog(
			org.OrgID(),
			ticketer.ID(),
			httpLog.URL,
			httpLog.Request,
			httpLog.Response,
			httpLog.Status != flows.CallStatusSuccess,
			time.Duration(httpLog.ElapsedMS)*time.Millisecond,
			httpLog.CreatedOn,
		))

		InsertHTTPLogs(ctx, db, httpLogs)
	}

	return err
}

const selectOpenTicketSQL = `
SELECT
  t.id AS id,
  t.uuid AS uuid,
  t.org_id AS org_id,
  t.contact_id AS contact_id,
  t.ticketer_id AS ticketer_id,
  t.external_id AS external_id,
  t.status AS status,
  t.subject AS subject,
  t.body AS body,
  t.config AS config,
  t.opened_on AS opened_on,
  t.modified_on AS modified_on,
  t.closed_on AS closed_on
FROM
  tickets_ticket t
WHERE
  t.org_id = $1 AND
  t.contact_id = $2 AND
  t.status = 'O'
`

// TicketsOpenForContact looks up the open tickets for the passed in contact
func TicketsOpenForContact(ctx context.Context, db Queryer, org *OrgAssets, contact *Contact) ([]*Ticket, error) {
	rows, err := db.QueryxContext(ctx, selectOpenTicketSQL, org.OrgID(), contact.ID())
	if err != nil && err != sql.ErrNoRows {
		return nil, errors.Wrapf(err, "error querying for open tickets for contact: %d", contact.ID())
	}
	defer rows.Close()

	tickets := make([]*Ticket, 0, 2)
	for rows.Next() {
		ticket := &Ticket{}
		err = rows.StructScan(&ticket.t)
		if err != nil {
			return nil, errors.Wrapf(err, "error unmarshalling ticket")
		}
		tickets = append(tickets, ticket)
	}

	return tickets, nil
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
func LookupTicketByUUID(ctx context.Context, db Queryer, uuid flows.TicketUUID) (*Ticket, error) {
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
  status = $2,
  config = $3
WHERE
  id = $1
`

// UpdateTicket updates the passed in ticket with the passed in external id, status and config
func UpdateTicket(ctx context.Context, db Queryer, ticket *Ticket, status TicketStatus, config map[string]string) error {
	t := &ticket.t
	t.Status = status

	for key, value := range config {
		t.Config.Map()[key] = value
	}

	return Exec(ctx, "update ticket", db, updateTicketSQL, t.ID, t.Status, t.Config)
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
func (t *Ticketer) AsService(httpClient *http.Client, httpRetries *httpx.RetryConfig, ticketer *flows.Ticketer) (TicketService, error) {
	initFunc := ticketServices[t.Type()]
	if initFunc != nil {
		return initFunc(httpClient, httpRetries, ticketer, t.t.Config)
	}

	return nil, errors.Errorf("unrecognized ticket service type '%s'", t.Type())
}

type TicketService interface {
	flows.TicketService

	Forward(ticket *Ticket, text string, logHTTP flows.HTTPLogCallback) error
}

type TicketServiceFunc func(httpClient *http.Client, httpRetries *httpx.RetryConfig, ticketer *flows.Ticketer, config map[string]string) (TicketService, error)

var ticketServices = map[string]TicketServiceFunc{}

func RegisterTicketService(name string, initFunc TicketServiceFunc) {
	ticketServices[name] = initFunc
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
	if err != nil && err != sql.ErrNoRows {
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
