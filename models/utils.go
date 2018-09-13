package models

import (
	"encoding/json"

	"github.com/jmoiron/sqlx"
	"github.com/juju/errors"
)

func readJSONRow(rows *sqlx.Rows, destination interface{}) error {
	var jsonBlob string
	err := rows.Scan(&jsonBlob)
	if err != nil {
		return errors.Annotate(err, "error scanning row json")
	}

	err = json.Unmarshal([]byte(jsonBlob), destination)
	if err != nil {
		return errors.Annotate(err, "error unmarshalling row json")
	}

	return nil
}
