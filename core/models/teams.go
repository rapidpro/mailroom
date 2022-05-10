package models

import (
	"database/sql/driver"

	"github.com/nyaruka/null"
)

const (
	// NilTeamID is the id 0 considered as nil user id
	NilTeamID = TeamID(0)
)

// TeamID is our type for team ids, which can be null
type TeamID null.Int

type TeamUUID string

type Team struct {
	ID   TeamID   `json:"id"`
	UUID TeamUUID `json:"uuid"`
	Name string   `json:"name"`
}

// MarshalJSON marshals into JSON. 0 values will become null
func (i TeamID) MarshalJSON() ([]byte, error) {
	return null.Int(i).MarshalJSON()
}

// UnmarshalJSON unmarshals from JSON. null values become 0
func (i *TeamID) UnmarshalJSON(b []byte) error {
	return null.UnmarshalInt(b, (*null.Int)(i))
}

// Value returns the db value, null is returned for 0
func (i TeamID) Value() (driver.Value, error) {
	return null.Int(i).Value()
}

// Scan scans from the db value. null values become 0
func (i *TeamID) Scan(value interface{}) error {
	return null.ScanInt(value, (*null.Int)(i))
}
