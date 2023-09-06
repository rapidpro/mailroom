package models

import (
	"database/sql/driver"

	"github.com/nyaruka/null/v3"
)

const (
	// NilTeamID is the id 0 considered as nil user id
	NilTeamID = TeamID(0)
)

// TeamID is our type for team ids, which can be null
type TeamID int

type TeamUUID string

type Team struct {
	ID   TeamID   `json:"id"`
	UUID TeamUUID `json:"uuid"`
	Name string   `json:"name"`
}

func (i *TeamID) Scan(value any) error         { return null.ScanInt(value, i) }
func (i TeamID) Value() (driver.Value, error)  { return null.IntValue(i) }
func (i *TeamID) UnmarshalJSON(b []byte) error { return null.UnmarshalInt(b, i) }
func (i TeamID) MarshalJSON() ([]byte, error)  { return null.MarshalInt(i) }
