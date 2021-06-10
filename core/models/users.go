package models

import (
	"database/sql/driver"

	"github.com/nyaruka/null"
)

const (
	// NilUserID is the id 0 considered as nil user id
	NilUserID = UserID(0)
)

// UserID is our type for user ids, which can be null
type UserID null.Int

// MarshalJSON marshals into JSON. 0 values will become null
func (i UserID) MarshalJSON() ([]byte, error) {
	return null.Int(i).MarshalJSON()
}

// UnmarshalJSON unmarshals from JSON. null values become 0
func (i *UserID) UnmarshalJSON(b []byte) error {
	return null.UnmarshalInt(b, (*null.Int)(i))
}

// Value returns the db value, null is returned for 0
func (i UserID) Value() (driver.Value, error) {
	return null.Int(i).Value()
}

// Scan scans from the db value. null values become 0
func (i *UserID) Scan(value interface{}) error {
	return null.ScanInt(value, (*null.Int)(i))
}
