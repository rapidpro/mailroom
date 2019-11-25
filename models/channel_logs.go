package models

import (
	"context"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

// ChannelLogID is our type for a channel log id
type ChannelLogID int64

// ChannelLog is the mailroom struct that represents channel logs
type ChannelLog struct {
	// inner struct for privacy and so we don't collide with method names
	l struct {
		ID           ChannelLogID `db:"id"`
		Description  string       `db:"description"`
		IsError      bool         `db:"is_error"`
		URL          string       `db:"url"`
		Method       string       `db:"method"`
		Request      string       `db:"request"`
		Response     string       `db:"response"`
		Status       int          `db:"response_status"`
		CreatedOn    time.Time    `db:"created_on"`
		RequestTime  int          `db:"request_time"`
		ChannelID    ChannelID    `db:"channel_id"`
		ConnectionID ConnectionID `db:"connection_id"`
	}
}

// ID returns the id of this channel log
func (l *ChannelLog) ID() ChannelLogID { return l.l.ID }

const insertChannelLogSQL = `
INSERT INTO
	channels_channellog(
		description, is_error, url, method, request, response, response_status,
		created_on, request_time, channel_id, connection_id)
	VALUES(
		:description, :is_error, :url, :method, :request, :response, :response_status,
		:created_on, :request_time, :channel_id, :connection_id)

RETURNING 
	id as id
`

// InsertChannelLog writes a channel log to the db returning the inserted log
func InsertChannelLog(ctx context.Context, db *sqlx.DB,
	desc string, isError bool, method string, url string, request []byte, status int, response []byte,
	createdOn time.Time, elapsed time.Duration, channel *Channel, conn *ChannelConnection) (*ChannelLog, error) {

	log := &ChannelLog{}
	l := &log.l

	l.Description = desc
	l.IsError = isError
	l.URL = url
	l.Method = method
	l.Request = string(request)
	l.Response = string(response)
	l.Status = status
	l.CreatedOn = createdOn
	l.RequestTime = int(elapsed / time.Millisecond)
	l.ChannelID = channel.ID()

	if conn != nil {
		l.ConnectionID = conn.ID()
	}

	err := BulkSQL(ctx, "insert channel log", db, insertChannelLogSQL, []interface{}{l})
	if err != nil {
		return nil, errors.Wrapf(err, "error inserting channel log")
	}
	return log, nil
}
