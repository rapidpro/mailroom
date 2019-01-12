package models

import (
	"context"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/nyaruka/goflow/flows"
	"github.com/pkg/errors"
)

type ChannelSessionID int64

type ChannelSessionStatus string

type ChannelSessionDirection string

type ChannelSessionType string

const (
	ChannelSessionDirectionIn  = ChannelSessionDirection("I")
	ChannelSessionDirectionOut = ChannelSessionDirection("O")

	ChannelSessionTypeIVR = ChannelSessionType("F")

	ChannelSessionStatusPending    = ChannelSessionStatus("P")
	ChannelSessionStatusQueued     = ChannelSessionStatus("Q")
	ChannelSessionStatusWired      = ChannelSessionStatus("W")
	ChannelSessionStatusRinging    = ChannelSessionStatus("R")
	ChannelSessionStatusInProgress = ChannelSessionStatus("I")
	ChannelSessionStatusBusy       = ChannelSessionStatus("B")
	ChannelSessionStatusFailed     = ChannelSessionStatus("F")
	ChannelSessionStatusErrored    = ChannelSessionStatus("E")
	ChannelSessionStatusNoAnswer   = ChannelSessionStatus("N")
	ChannelSessionStatusCancelled  = ChannelSessionStatus("C")
	ChannelSessionStatusCompleted  = ChannelSessionStatus("D")
)

type ChannelSession struct {
	s struct {
		ID           ChannelSessionID        `json:"id"             db:"id"`
		IsActive     bool                    `json:"is_active"      db:"is_active"`
		CreatedOn    time.Time               `json:"created_on"     db:"created_on"`
		ModifiedOn   time.Time               `json:"modified_on"    db:"modified_on"`
		ExternalID   string                  `json:"external_id"    db:"external_id"`
		Status       ChannelSessionStatus    `json:"status"         db:"status"`
		Direction    ChannelSessionDirection `json:"direction"      db:"direction"`
		StartedOn    *time.Time              `json:"started_on"     db:"started_on"`
		EndedOn      *time.Time              `json:"ended_on"       db:"ended_on"`
		SessionType  ChannelSessionType      `json:"session_type"   db:"session_type"`
		Duration     int                     `json:"duration"       db:"duration"`
		RetryCount   int                     `json:"retry_count"    db:"retry_count"`
		NextAttempt  *time.Time              `json:"next_attempt"   db:"next_attempt"`
		ChannelID    ChannelID               `json:"channel_id"     db:"channel_id"`
		ContactID    flows.ContactID         `json:"contact_id"     db:"contact_id"`
		ContactURNID URNID                   `json:"contact_urn_id" db:"contact_urn_id"`
		OrgID        OrgID                   `json:"org_id"         db:"org_id"`
		ErrorCount   int                     `json:"error_count"    db:"error_count"`
	}
}

func (s *ChannelSession) ID() ChannelSessionID         { return s.s.ID }
func (s *ChannelSession) Status() ChannelSessionStatus { return s.s.Status }
func (s *ChannelSession) ExternalID() string           { return s.s.ExternalID }
func (s *ChannelSession) OrgID() OrgID                 { return s.s.OrgID }
func (s *ChannelSession) ContactID() flows.ContactID   { return s.s.ContactID }
func (s *ChannelSession) ContactURNID() URNID          { return s.s.ContactURNID }
func (s *ChannelSession) ChannelID() ChannelID         { return s.s.ChannelID }

const insertChannelSessionSQL = `
INSERT INTO
	channels_channelsession
(
	is_active,
	created_on,
	modified_on,
	external_id,
	status,
	direction,
	session_type,
	duration,
	org_id,
	channel_id,
	contact_id,
	contact_urn_id,
	error_count,
	retry_count
)

VALUES(
	:is_active,
	NOW(),
	NOW(),
	:external_id,
	:status,
	:direction,
	:session_type,
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

// CreateIVRSession creates a new IVR session for the passed in org, channel and contact, inserting it
func CreateIVRSession(ctx context.Context, db *sqlx.DB, orgID OrgID, channelID ChannelID, contactID flows.ContactID, urnID URNID,
	direction ChannelSessionDirection, status ChannelSessionStatus, externalID string) (*ChannelSession, error) {

	session := &ChannelSession{}

	s := &session.s
	s.IsActive = true
	s.OrgID = orgID
	s.ChannelID = channelID
	s.ContactID = contactID
	s.ContactURNID = urnID
	s.Direction = direction
	s.Status = status
	s.SessionType = ChannelSessionTypeIVR
	s.ExternalID = externalID

	rows, err := db.NamedQueryContext(ctx, insertChannelSessionSQL, s)
	if err != nil {
		return nil, errors.Wrapf(err, "error inserting new channel session")
	}

	rows.Next()

	now := time.Now()
	err = rows.Scan(&s.ID, &now)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to scan id and now for new channel session")
	}

	// set our created and modified the same as the DB
	s.CreatedOn = now
	s.ModifiedOn = now

	return session, nil
}

const selectChannelSessionSQL = `
SELECT
	id, 
	is_active, 
	created_on, 
	modified_on, 
	external_id, 
	status, 
	direction, 
	started_on, 
	ended_on, 
	session_type, 
	duration, 
	retry_count, 
	next_attempt, 
	channel_id, 
	contact_id, 
	contact_urn_id, 
	org_id, 
	error_count
FROM
	channels_channelsession
WHERE
	id = $1 AND
	is_active = TRUE
`

// LoadChannelSession loads a channel session by id
func LoadChannelSession(ctx context.Context, db Queryer, id ChannelSessionID) (*ChannelSession, error) {
	session := &ChannelSession{}
	err := db.GetContext(ctx, &session.s, selectChannelSessionSQL, id)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to load channel connection widh id: %d", id)
	}
	return session, nil
}

// UpdateExternalID updates the external id on the passed in channel session
func (s *ChannelSession) UpdateExternalID(ctx context.Context, db *sqlx.DB, id string) error {
	s.s.ExternalID = id
	s.s.Status = ChannelSessionStatusWired

	_, err := db.ExecContext(ctx, `
	UPDATE channels_channelsession SET external_id = $2, status = $3, modified_on = NOW() WHERE id = $1
	`, s.s.ID, s.s.ExternalID, s.s.Status)

	if err != nil {
		return errors.Wrapf(err, "error updating external id to: %s for channel session: %d", s.s.ExternalID, s.s.ID)
	}

	return nil
}

// UpdateStatus updates the status for this session
func (s *ChannelSession) UpdateStatus(ctx context.Context, db Queryer, status ChannelSessionStatus, duration int) error {
	s.s.Status = status
	var err error

	// only write a duration if it is greater than 0
	if duration > 0 {
		s.s.Duration = duration
		_, err = db.ExecContext(ctx,
			`UPDATE channels_channelsession SET status = $2, duration = $3, modified_on = NOW() WHERE id = $1`,
			s.s.ID, s.s.Status, s.s.Duration,
		)
	} else {
		_, err = db.ExecContext(ctx,
			`UPDATE channels_channelsession SET status = $2, modified_on = NOW() WHERE id = $1`,
			s.s.ID, s.s.Status,
		)
	}

	if err != nil {
		return errors.Wrapf(err, "error updating status for channel session: %d", s.s.ID)
	}

	return nil
}

// UpdateChannelConnectionStatuses updates the status for all the passed in connection ids
func UpdateChannelConnectionStatuses(ctx context.Context, db Queryer, connectionIDs []ChannelSessionID, status ChannelSessionStatus) error {
	if len(connectionIDs) == 0 {
		return nil
	}
	_, err := db.ExecContext(ctx, `
	UPDATE channels_channelsession SET status = $2, modified_on = NOW() WHERE id = ANY($1)`,
		pq.Array(connectionIDs), status)

	if err != nil {
		return errors.Wrapf(err, "error updating channel connection statuses")
	}

	return nil
}
