package dbutil

import (
	"encoding/json"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"gopkg.in/go-playground/validator.v9"
)

var validate = validator.New()

// ReadJSONRow reads a row which is JSON into a destination struct
func ReadJSONRow(rows *sqlx.Rows, destination interface{}) error {
	var jsonBlob string
	err := rows.Scan(&jsonBlob)
	if err != nil {
		return errors.Wrap(err, "error scanning row JSON")
	}

	err = json.Unmarshal([]byte(jsonBlob), destination)
	if err != nil {
		return errors.Wrap(err, "error unmarshalling row JSON")
	}

	// validate our final struct
	err = validate.Struct(destination)
	if err != nil {
		return errors.Wrapf(err, "failed validation for JSON: %s", jsonBlob)
	}

	return nil
}
