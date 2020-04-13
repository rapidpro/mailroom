package models

import (
	"context"
	"database/sql/driver"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/nyaruka/null"
	"github.com/pkg/errors"
)

type ConnectionID null.Int

const NilConnectionID = ConnectionID(0)

type ConnectionStatus string

type ConnectionDirection string

type ConnectionType string

const (
	ConnectionDirectionIn  = ConnectionDirection("I")
	ConnectionDirectionOut = ConnectionDirection("O")

	ConnectionTypeIVR = ConnectionType("V")

	ConnectionStatusPending    = ConnectionStatus("P")
	ConnectionStatusQueued     = ConnectionStatus("Q")
	ConnectionStatusWired      = ConnectionStatus("W")
	ConnectionStatusRinging    = ConnectionStatus("R")
	ConnectionStatusInProgress = ConnectionStatus("I")
	ConnectionStatusBusy       = ConnectionStatus("B")
	ConnectionStatusFailed     = ConnectionStatus("F")
	ConnectionStatusErrored    = ConnectionStatus("E")
	ConnectionStatusNoAnswer   = ConnectionStatus("N")
	ConnectionStatusCancelled  = ConnectionStatus("C")
	ConnectionStatusCompleted  = ConnectionStatus("D")

	ConnectionMaxRetries = 3

	// ConnectionRetryWait is our default wait to retry connections
	ConnectionRetryWait = time.Minute * 60

	// ConnectionThrottleWait is our wait between throttle retries
	ConnectionThrottleWait = time.Minute * 2
)

type ChannelConnection struct {
	c struct {
		ID             ConnectionID        `json:"id"              db:"id"`
		CreatedOn      time.Time           `json:"created_on"      db:"created_on"`
		ModifiedOn     time.Time           `json:"modified_on"     db:"modified_on"`
		ExternalID     string              `json:"external_id"     db:"external_id"`
		Status         ConnectionStatus    `json:"status"          db:"status"`
		Direction      ConnectionDirection `json:"direction"       db:"direction"`
		StartedOn      *time.Time          `json:"started_on"      db:"started_on"`
		EndedOn        *time.Time          `json:"ended_on"        db:"ended_on"`
		ConnectionType ConnectionType      `json:"connection_type" db:"connection_type"`
		Duration       int                 `json:"duration"        db:"duration"`
		RetryCount     int                 `json:"retry_count"     db:"retry_count"`
		NextAttempt    *time.Time          `json:"next_attempt"    db:"next_attempt"`
		ChannelID      ChannelID           `json:"channel_id"      db:"channel_id"`
		ContactID      ContactID           `json:"contact_id"      db:"contact_id"`
		ContactURNID   URNID               `json:"contact_urn_id"  db:"contact_urn_id"`
		OrgID          OrgID               `json:"org_id"          db:"org_id"`
		ErrorCount     int                 `json:"error_count"     db:"error_count"`
		StartID        StartID             `json:"start_id"        db:"start_id"`
	}
}

func (c *ChannelConnection) ID() ConnectionID         { return c.c.ID }
func (c *ChannelConnection) Status() ConnectionStatus { return c.c.Status }
func (c *ChannelConnection) NextAttempt() *time.Time  { return c.c.NextAttempt }
func (c *ChannelConnection) ExternalID() string       { return c.c.ExternalID }
func (c *ChannelConnection) OrgID() OrgID             { return c.c.OrgID }
func (c *ChannelConnection) ContactID() ContactID     { return c.c.ContactID }
func (c *ChannelConnection) ContactURNID() URNID      { return c.c.ContactURNID }
func (c *ChannelConnection) ChannelID() ChannelID     { return c.c.ChannelID }
func (c *ChannelConnection) StartID() StartID         { return c.c.StartID }

const insertConnectionSQL = `
INSERT INTO
	channels_channelconnection
(
	created_on,
	modified_on,
	external_id,
	status,
	direction,
	connection_type,
	duration,
	org_id,
	channel_id,
	contact_id,
	contact_urn_id,
	error_count,
	retry_count
)

VALUES(
	NOW(),
	NOW(),
	:external_id,
	:status,
	:direction,
	:connection_type,
	0,
	:org_id,
	:channel_id,
	:contact_id,
	:contact_urn_id,
	0,
	0
)
RETURNING
	id,
	NOW();
`

// InsertIVRConnection creates a new IVR session for the passed in org, channel and contact, inserting it
func InsertIVRConnection(ctx context.Context, db *sqlx.DB, orgID OrgID, channelID ChannelID, startID StartID, contactID ContactID, urnID URNID,
	direction ConnectionDirection, status ConnectionStatus, externalID string) (*ChannelConnection, error) {

	connection := &ChannelConnection{}

	c := &connection.c
	c.OrgID = orgID
	c.ChannelID = channelID
	c.ContactID = contactID
	c.ContactURNID = urnID
	c.Direction = direction
	c.Status = status
	c.ConnectionType = ConnectionTypeIVR
	c.ExternalID = externalID
	c.StartID = startID

	rows, err := db.NamedQueryContext(ctx, insertConnectionSQL, c)
	if err != nil {
		return nil, errors.Wrapf(err, "error inserting new channel connection")
	}
	defer rows.Close()

	rows.Next()

	now := time.Now()
	err = rows.Scan(&c.ID, &now)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to scan id and now for new channel session")
	}

	// add a many to many for our start if set
	if startID != NilStartID {
		_, err := db.ExecContext(
			ctx,
			`INSERT INTO flows_flowstart_connections(flowstart_id, channelconnection_id) VALUES($1, $2) ON CONFLICT DO NOTHING`,
			startID, c.ID,
		)
		if err != nil {
			return nil, errors.Wrapf(err, "unable to add start association for channelconnection")
		}
	}

	// set our created and modified the same as the DB
	c.CreatedOn = now
	c.ModifiedOn = now

	return connection, nil
}

const selectConnectionSQL = `
SELECT
	cc.id as id, 
	cc.created_on as created_on, 
	cc.modified_on as modified_on, 
	cc.external_id as external_id,  
	cc.status as status, 
	cc.direction as direction, 
	cc.started_on as started_on, 
	cc.ended_on as ended_on, 
	cc.connection_type as connection_type, 
	cc.duration as duration, 
	cc.retry_count as retry_count, 
	cc.next_attempt as next_attempt, 
	cc.channel_id as channel_id, 
	cc.contact_id as contact_id, 
	cc.contact_urn_id as contact_urn_id, 
	cc.org_id as org_id, 
	cc.error_count as error_count, 
	fsc.flowstart_id as start_id
FROM
	channels_channelconnection as cc
	LEFT OUTER JOIN flows_flowstart_connections fsc ON cc.id = fsc.channelconnection_id
WHERE
	cc.id = $1
`

// SelectChannelConnection loads a channel connection by id
func SelectChannelConnection(ctx context.Context, db Queryer, id ConnectionID) (*ChannelConnection, error) {
	conn := &ChannelConnection{}
	err := db.GetContext(ctx, &conn.c, selectConnectionSQL, id)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to load channel connection with id: %d", id)
	}
	return conn, nil
}

const selectConnectionByExternalIDSQL = `
SELECT
	cc.id as id, 
	cc.created_on as created_on, 
	cc.modified_on as modified_on, 
	cc.external_id as external_id,  
	cc.status as status, 
	cc.direction as direction, 
	cc.started_on as started_on, 
	cc.ended_on as ended_on, 
	cc.connection_type as connection_type, 
	cc.duration as duration, 
	cc.retry_count as retry_count, 
	cc.next_attempt as next_attempt, 
	cc.channel_id as channel_id, 
	cc.contact_id as contact_id, 
	cc.contact_urn_id as contact_urn_id, 
	cc.org_id as org_id, 
	cc.error_count as error_count, 
	fsc.flowstart_id as start_id
FROM
	channels_channelconnection as cc
	LEFT OUTER JOIN flows_flowstart_connections fsc ON cc.id = fsc.channelconnection_id
WHERE
	cc.channel_id = $1 AND
	cc.connection_type = $2 AND
	cc.external_id = $3
`

// SelectChannelConnectionByExternalID loads a channel connection by id
func SelectChannelConnectionByExternalID(ctx context.Context, db Queryer, channelID ChannelID, connType ConnectionType, externalID string) (*ChannelConnection, error) {
	conn := &ChannelConnection{}
	err := db.GetContext(ctx, &conn.c, selectConnectionByExternalIDSQL, channelID, connType, externalID)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to load channel connection with external id: %s", externalID)
	}
	return conn, nil
}

const selectRetryConnectionsSQL = `
SELECT
	cc.id as id, 
	cc.created_on as created_on, 
	cc.modified_on as modified_on, 
	cc.external_id as external_id,  
	cc.status as status, 
	cc.direction as direction, 
	cc.started_on as started_on, 
	cc.ended_on as ended_on, 
	cc.connection_type as connection_type, 
	cc.duration as duration, 
	cc.retry_count as retry_count, 
	cc.next_attempt as next_attempt, 
	cc.channel_id as channel_id, 
	cc.contact_id as contact_id, 
	cc.contact_urn_id as contact_urn_id, 
	cc.org_id as org_id, 
	cc.error_count as error_count, 
	fsc.flowstart_id as start_id
FROM
	channels_channelconnection as cc
	LEFT OUTER JOIN flows_flowstart_connections fsc ON cc.id = fsc.channelconnection_id
WHERE
	cc.connection_type = 'V' AND
	next_attempt < NOW() AND
	(cc.status = 'E' OR cc.status = 'Q')
ORDER BY 
	cc.next_attempt ASC
LIMIT
    $1
`

// LoadChannelConnectionsToRetry returns up to limit connections that need to be retried
func LoadChannelConnectionsToRetry(ctx context.Context, db Queryer, limit int) ([]*ChannelConnection, error) {
	rows, err := db.QueryxContext(ctx, selectRetryConnectionsSQL, limit)
	if err != nil {
		return nil, errors.Wrapf(err, "error selecting connections to retry")
	}
	defer rows.Close()

	conns := make([]*ChannelConnection, 0, 10)
	for rows.Next() {
		conn := &ChannelConnection{}
		err = rows.StructScan(&conn.c)
		if err != nil {
			return nil, errors.Wrapf(err, "error scanning channel connection")
		}
		conns = append(conns, conn)
	}

	return conns, nil
}

// UpdateExternalID updates the external id on the passed in channel session
func (c *ChannelConnection) UpdateExternalID(ctx context.Context, db *sqlx.DB, id string) error {
	c.c.ExternalID = id
	c.c.Status = ConnectionStatusWired

	_, err := db.ExecContext(ctx, `
	UPDATE channels_channelconnection SET external_id = $2, status = $3, modified_on = NOW() WHERE id = $1
	`, c.c.ID, c.c.ExternalID, c.c.Status)

	if err != nil {
		return errors.Wrapf(err, "error updating external id to: %s for channel connection: %d", c.c.ExternalID, c.c.ID)
	}

	return nil
}

// MarkStarted updates the status for this connection as well as sets the started on date
func (c *ChannelConnection) MarkStarted(ctx context.Context, db Queryer, now time.Time) error {
	c.c.Status = ConnectionStatusInProgress
	c.c.StartedOn = &now

	_, err := db.ExecContext(ctx, `
	UPDATE channels_channelconnection SET status = $2, started_on = $3, modified_on = NOW() WHERE id = $1
	`, c.c.ID, c.c.Status, c.c.StartedOn)

	if err != nil {
		return errors.Wrapf(err, "error marking channel connection as started")
	}

	return nil
}

// MarkErrored updates the status for this connection to errored and schedules a retry if appropriate
func (c *ChannelConnection) MarkErrored(ctx context.Context, db Queryer, now time.Time, wait time.Duration) error {
	c.c.Status = ConnectionStatusErrored
	c.c.EndedOn = &now

	if c.c.RetryCount < ConnectionMaxRetries {
		c.c.RetryCount++
		next := now.Add(wait)
		c.c.NextAttempt = &next
	} else {
		c.c.Status = ConnectionStatusFailed
		c.c.NextAttempt = nil
	}

	_, err := db.ExecContext(ctx,
		`UPDATE channels_channelconnection SET status = $2, ended_on = $3, retry_count = $4, next_attempt = $5, modified_on = NOW() WHERE id = $1`,
		c.c.ID, c.c.Status, c.c.EndedOn, c.c.RetryCount, c.c.NextAttempt,
	)

	if err != nil {
		return errors.Wrapf(err, "error marking channel connection as errored")
	}

	return nil
}

// MarkFailed updates the status for this connection
func (c *ChannelConnection) MarkFailed(ctx context.Context, db Queryer, now time.Time) error {
	c.c.Status = ConnectionStatusFailed
	c.c.EndedOn = &now

	_, err := db.ExecContext(ctx,
		`UPDATE channels_channelconnection SET status = $2, ended_on = $3, modified_on = NOW() WHERE id = $1`,
		c.c.ID, c.c.Status, c.c.EndedOn,
	)

	if err != nil {
		return errors.Wrapf(err, "error marking channel connection as failed")
	}

	return nil
}

// MarkThrottled updates the status for this connection to be queued, to be retried in a minute
func (c *ChannelConnection) MarkThrottled(ctx context.Context, db Queryer, now time.Time) error {
	c.c.Status = ConnectionStatusQueued
	next := now.Add(ConnectionThrottleWait)
	c.c.NextAttempt = &next

	_, err := db.ExecContext(ctx,
		`UPDATE channels_channelconnection SET status = $2, next_attempt = $3, modified_on = NOW() WHERE id = $1`,
		c.c.ID, c.c.Status, c.c.NextAttempt,
	)

	if err != nil {
		return errors.Wrapf(err, "error marking channel connection as throttled")
	}

	return nil
}

// UpdateStatus updates the status for this connection
func (c *ChannelConnection) UpdateStatus(ctx context.Context, db Queryer, status ConnectionStatus, duration int, now time.Time) error {
	c.c.Status = status
	var err error

	// only write a duration if it is greater than 0
	if duration > 0 {
		c.c.Duration = duration
		c.c.EndedOn = &now
		_, err = db.ExecContext(ctx,
			`UPDATE channels_channelconnection SET status = $2, duration = $3, ended_on = $4, modified_on = NOW() WHERE id = $1`,
			c.c.ID, c.c.Status, c.c.Duration, c.c.EndedOn,
		)
	} else {
		_, err = db.ExecContext(ctx,
			`UPDATE channels_channelconnection SET status = $2, modified_on = NOW() WHERE id = $1`,
			c.c.ID, c.c.Status,
		)
	}

	if err != nil {
		return errors.Wrapf(err, "error updating status for channel connection: %d", c.c.ID)
	}

	return nil
}

// UpdateChannelConnectionStatuses updates the status for all the passed in connection ids
func UpdateChannelConnectionStatuses(ctx context.Context, db Queryer, connectionIDs []ConnectionID, status ConnectionStatus) error {
	if len(connectionIDs) == 0 {
		return nil
	}
	_, err := db.ExecContext(ctx,
		`UPDATE channels_channelconnection SET status = $2, modified_on = NOW() WHERE id = ANY($1)`,
		pq.Array(connectionIDs), status,
	)

	if err != nil {
		return errors.Wrapf(err, "error updating channel connection statuses")
	}

	return nil
}

const selectActiveConnectionCountSQL = `
SELECT 
	count(*)
FROM 
	channels_channelconnection
WHERE
	channel_id = $1 AND
	(status = 'W' OR status = 'R' OR status = 'I')
`

// ActiveChannelConnectionCount returns the number of ongoing connections for the passed in channel
func ActiveChannelConnectionCount(ctx context.Context, db Queryer, id ChannelID) (int, error) {
	count := 0
	err := db.GetContext(ctx, &count, selectActiveConnectionCountSQL, id)
	if err != nil {
		return 0, errors.Wrapf(err, "unable to select active channel connection count")
	}
	return count, nil
}

// MarshalJSON marshals into JSON. 0 values will become null
func (i ConnectionID) MarshalJSON() ([]byte, error) {
	return null.Int(i).MarshalJSON()
}

// UnmarshalJSON unmarshals from JSON. null values become 0
func (i *ConnectionID) UnmarshalJSON(b []byte) error {
	return null.UnmarshalInt(b, (*null.Int)(i))
}

// Value returns the db value, null is returned for 0
func (i ConnectionID) Value() (driver.Value, error) {
	return null.Int(i).Value()
}

// Scan scans from the db value. null values become 0
func (i *ConnectionID) Scan(value interface{}) error {
	return null.ScanInt(value, (*null.Int)(i))
}
