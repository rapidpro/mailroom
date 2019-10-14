package models

import (
	"context"
	"database/sql/driver"
	"time"

	"github.com/nyaruka/null"
)

// HTTPLogID is our type for classifier ids
type HTTPLogID null.Int

// HTTPLogType is the type for the type of log this is
type HTTPLogType string

const (
	// LogTypeIntentsSynced is our type for when we sync intents
	LogTypeIntentsSynced = "intents_synced"

	// LogTypeClassifierCalled is our type for when we call a classifier
	LogTypeClassifierCalled = "classifier_called"
)

// HTTPLog is our type for a HTTPLog
type HTTPLog struct {
	h struct {
		ID           HTTPLogID    `db:"id"`
		LogType      HTTPLogType  `db:"log_type"`
		ClassifierID ClassifierID `db:"classifier_id"`
		URL          string       `db:"url"`
		Request      string       `db:"request"`
		Response     string       `db:"response"`
		IsError      bool         `db:"is_error"`
		RequestTime  int          `db:"request_time"`
		CreatedOn    time.Time    `db:"created_on"`
		OrgID        OrgID        `db:"org_id"`
	}
}

// NewClassifierCalledLog creates a new HTTPLog returning the result
func NewClassifierCalledLog(
	orgID OrgID, cid ClassifierID, url string, request string, response string,
	isError bool, elapsed time.Duration, createdOn time.Time) *HTTPLog {
	h := &HTTPLog{}
	h.h.LogType = LogTypeClassifierCalled
	h.h.ClassifierID = cid
	h.h.OrgID = orgID
	h.h.URL = url
	h.h.Request = request
	h.h.Response = response
	h.h.IsError = isError
	h.h.RequestTime = int(elapsed / time.Millisecond)
	h.h.CreatedOn = createdOn
	return h
}

const insertHTTPLogsSQL = `
INSERT INTO classifiers_classifierlog( url,  request,  response,  is_error,  description,  request_time,  created_on,  classifier_id,  org_id)
							   VALUES(:url, :request, :response, :is_error, :descripiton, :request_time, :created_on, :classifier_id, :org_id)
RETURNING id
`

// InsertHTTPLogs inserts the passed in classifiers returning any errors encountered
func InsertHTTPLogs(ctx context.Context, tx Queryer, logs []*HTTPLog) error {
	if len(logs) == 0 {
		return nil
	}

	ls := make([]interface{}, len(logs))
	for i := range logs {
		ls[i] = &logs[i].h
	}

	return BulkSQL(ctx, "inserted http logs", tx, insertHTTPLogsSQL, ls)
}

// MarshalJSON marshals into JSON. 0 values will become null
func (i HTTPLogID) MarshalJSON() ([]byte, error) {
	return null.Int(i).MarshalJSON()
}

// UnmarshalJSON unmarshals from JSON. null values become 0
func (i *HTTPLogID) UnmarshalJSON(b []byte) error {
	return null.UnmarshalInt(b, (*null.Int)(i))
}

// Value returns the db value, null is returned for 0
func (i HTTPLogID) Value() (driver.Value, error) {
	return null.Int(i).Value()
}

// Scan scans from the db value. null values become 0
func (i *HTTPLogID) Scan(value interface{}) error {
	return null.ScanInt(value, (*null.Int)(i))
}
