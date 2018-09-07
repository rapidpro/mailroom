package models

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/flows"
	null "gopkg.in/guregu/null.v3"
)

type MsgID int64
type MsgDirection string
type MsgStatus string
type MsgVisibility string
type MsgType string
type ChannelID int
type ConnectionID null.Int
type ContactURNID int
type TopUpID null.Int

const StatusQueued = MsgStatus("Q")
const StatusPending = MsgStatus("P")

// TODO: response_to_id, response_to_external_id
// TODO: real tps_cost
// TODO: urn auth

type Msg struct {
	ID           MsgID             `db:"id"              json:"id"`
	UUID         flows.MsgUUID     `db:"uuid"            json:"uuid"`
	Text         string            `db:"text"            json:"text"`
	HighPriority bool              `db:"high_priority"   json:"high_priority"`
	CreatedOn    time.Time         `db:"created_on"      json:"created_on"`
	ModifiedOn   time.Time         `db:"modified_on"     json:"modified_on"`
	SentOn       time.Time         `db:"sent_on"         json:"sent_on"`
	QueuedOn     time.Time         `db:"queued_on"       json:"queued_on"`
	Direction    MsgDirection      `db:"direction"       json:"direction"`
	Status       MsgStatus         `db:"status"          json:"status"`
	Visibility   MsgVisibility     `db:"visibility"      json:"visibility"`
	MsgType      MsgType           `db:"msg_type"`
	MsgCount     int               `db:"msg_count"       json:"tps_cost"`
	ErrorCount   int               `db:"error_count"     json:"error_count"`
	NextAttempt  time.Time         `db:"next_attempt"    json:"next_attempt"`
	ExternalID   null.String       `db:"external_id"     json:"external_id"`
	Attachments  []string          `db:"attachments"     json:"attachments"`
	Metadata     null.String       `db:"metadata"        json:"metadata"`
	ChannelID    ChannelID         `db:"channel_id"      json:"channel_id"`
	ChannelUUID  flows.ChannelUUID `                     json:"channel_uuid"`
	ConnectionID ConnectionID      `db:"connection_id"`
	ContactID    ContactID         `db:"contact_id"      json:"contact_id"`
	ContactURNID ContactURNID      `db:"contact_urn_id"  json:"contact_urn_id"`
	URN          urns.URN          `                     json:"urn"`
	OrgID        OrgID             `db:"org_id"          json:"org_id"`
	TopUpID      TopUpID           `db:"topup_id"`
}

const insertMsgSQL = `
INSERT INTO
msgs_msg(uuid, text, high_priority, created_on, modified_on, direction, status, 
  	 	 visibility, msg_type, msg_count, error_count, next_attempt, channel_id, contact_id, contact_urn_id, org_id, topup_id)
  VALUES(:uuid, :text, :high_priority, NOW(), NOW(), :direction, :status,
         :visibility, :msg_type, :msg_count, :error_count, :next_attempt, :channel_id, :contact_id, :contact_urn_id, :org_id, :topup_id )
RETURNING id, NOW()
`

func CreateOutgoingMsg(ctx context.Context, tx *sqlx.Tx, org *OrgAssets, contactID ContactID, m *flows.MsgOut) (*Msg, error) {
	_, _, query, _ := m.URN().ToParts()
	parsedQuery, err := url.ParseQuery(query)
	if err != nil {
		return nil, err
	}

	// get the id of our URN
	idQuery := parsedQuery.Get("id")
	urnID, err := strconv.Atoi(idQuery)
	if urnID == 0 {
		return nil, fmt.Errorf("unable to create msg for URN, has no id: %s", m.URN())
	}

	// get the id of our active topup
	topupID, err := org.GetActiveTopup()
	if err != nil {
		return nil, fmt.Errorf("unable to create msg, no active topup: %s", err)
	}

	msg := &Msg{
		UUID:         m.UUID(),
		Text:         m.Text(),
		HighPriority: true,
		Direction:    MsgDirection("O"),
		Status:       StatusPending,
		Visibility:   MsgVisibility("V"),
		MsgType:      MsgType("M"),
		ContactID:    contactID,
		ContactURNID: ContactURNID(urnID),
		URN:          m.URN(),
		OrgID:        org.GetOrgID(),
		TopUpID:      topupID,
	}

	// TODO: calculate real msg count

	// set our channel id
	channelID, err := org.GetChannelID(m.Channel().UUID)
	if err != nil {
		return nil, fmt.Errorf("unable to find channel with UUID: %s", m.Channel().UUID)
	}
	msg.ChannelID = channelID
	msg.ChannelUUID = m.Channel().UUID

	// insert msg
	rows, err := tx.NamedQuery(insertMsgSQL, msg)
	if err != nil {
		return nil, err
	}
	rows.Next()
	var insertTime time.Time
	err = rows.Scan(&msg.ID, &insertTime)
	rows.Close()

	// populate our insert time
	if err != nil {
		msg.CreatedOn = insertTime
		msg.ModifiedOn = insertTime
	}

	// return it
	return msg, err
}

const queueMsgSQL = `
UPDATE 
	msgs_msg
SET 
	status = :status, 
	queued_on = :queued_on 
WHERE
	id = :id
`

func MarkMessageQueued(ctx context.Context, db *sqlx.DB, m *Msg) error {
	_, err := db.NamedExecContext(ctx, queueMsgSQL, m)
	return err
}
