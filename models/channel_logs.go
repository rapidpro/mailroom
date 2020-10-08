package models

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/gocommon/httpx"
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
	channels_channellog( description, is_error,  url,  method,  request,  response,  response_status,  created_on,  request_time,  channel_id,  connection_id)
	             VALUES(:description, :is_error, :url, :method, :request, :response, :response_status, :created_on, :request_time, :channel_id, :connection_id)
RETURNING 
	id as id
`

// NewChannelLog creates a new channel log
func NewChannelLog(trace *httpx.Trace, isError bool, desc string, channel *Channel, conn *ChannelConnection) *ChannelLog {
	log := &ChannelLog{}
	l := &log.l

	statusCode := 0
	if trace.Response != nil {
		statusCode = trace.Response.StatusCode
	}

	// if URL was rewritten (by nginx for example), we want to log the original request
	url := originalURL(trace.Request)

	l.Description = desc
	l.IsError = isError
	l.URL = url
	l.Method = trace.Request.Method
	l.Request = string(trace.RequestTrace)
	l.Response = trace.ResponseTraceUTF8("...")
	l.Status = statusCode
	l.CreatedOn = trace.StartTime
	l.RequestTime = int((trace.EndTime.Sub(trace.StartTime)) / time.Millisecond)
	l.ChannelID = channel.ID()
	if conn != nil {
		l.ConnectionID = conn.ID()
	}
	return log
}

func originalURL(r *http.Request) string {
	proxyPath := r.Header.Get("X-Forwarded-Path")
	if proxyPath != "" {
		return fmt.Sprintf("https://%s%s", r.Host, proxyPath)
	}
	return r.URL.String()
}

// InsertChannelLogs writes the given channel logs to the db
func InsertChannelLogs(ctx context.Context, db *sqlx.DB, logs []*ChannelLog) error {
	ls := make([]interface{}, len(logs))
	for i := range logs {
		ls[i] = &logs[i].l
	}

	err := BulkQuery(ctx, "insert channel log", db, insertChannelLogSQL, ls)
	if err != nil {
		return errors.Wrapf(err, "error inserting channel log")
	}
	return nil
}

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

	err := BulkQuery(ctx, "insert channel log", db, insertChannelLogSQL, []interface{}{l})
	if err != nil {
		return nil, errors.Wrapf(err, "error inserting channel log")
	}
	return log, nil
}
