package models

import (
	"context"
	"database/sql/driver"
	"time"

	"github.com/nyaruka/null"
)

// ClassifierLogID is our type for classifier ids
type ClassifierLogID null.Int

// ClassifierLog is our type for a ClassifierLog
type ClassifierLog struct {
	l struct {
		ID           ClassifierLogID `db:"id"`
		ClassifierID ClassifierID    `db:"classifier_id"`
		URL          string          `db:"url"`
		Request      string          `db:"request"`
		Response     string          `db:"response"`
		IsError      bool            `db:"is_error"`
		Description  string          `db:"description"`
		RequestTime  int             `db:"request_time"`
		CreatedOn    time.Time       `db:"created_on"`
	}
}

// NewClassifierLog creates a new ClassifierLog returning the result
func NewClassifierLog(
	cid ClassifierID, url string, request string, response string,
	isError bool, desc string, elapsed time.Duration, createdOn time.Time) *ClassifierLog {
	l := &ClassifierLog{}
	l.l.ClassifierID = cid
	l.l.URL = url
	l.l.Request = request
	l.l.Response = response
	l.l.IsError = isError
	l.l.Description = desc
	l.l.RequestTime = int(elapsed / time.Millisecond)
	l.l.CreatedOn = createdOn
	return l
}

const insertClassifierLogsSQL = `
INSERT INTO classifiers_classifierlog( url,  request,  response,  is_error,  description,  request_time,  created_on,  classifier_id)
							   VALUES(:url, :request, :response, :is_error, :descripiton, :request_time, :created_on, :classifier_id)
RETURNING id
`

// InsertClassifierLogs inserts the passed in classifiers returning any errors encountered
func InsertClassifierLogs(ctx context.Context, tx Queryer, logs []*ClassifierLog) error {
	if len(logs) == 0 {
		return nil
	}

	ls := make([]interface{}, len(logs))
	for i := range logs {
		ls[i] = &logs[i].l
	}

	return BulkSQL(ctx, "inserted classifier logs", tx, insertClassifierLogsSQL, ls)
}

// MarshalJSON marshals into JSON. 0 values will become null
func (i ClassifierLogID) MarshalJSON() ([]byte, error) {
	return null.Int(i).MarshalJSON()
}

// UnmarshalJSON unmarshals from JSON. null values become 0
func (i *ClassifierLogID) UnmarshalJSON(b []byte) error {
	return null.UnmarshalInt(b, (*null.Int)(i))
}

// Value returns the db value, null is returned for 0
func (i ClassifierLogID) Value() (driver.Value, error) {
	return null.Int(i).Value()
}

// Scan scans from the db value. null values become 0
func (i *ClassifierLogID) Scan(value interface{}) error {
	return null.ScanInt(value, (*null.Int)(i))
}
