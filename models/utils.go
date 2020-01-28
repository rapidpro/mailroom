package models

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"gopkg.in/go-playground/validator.v9"
)

var validate = validator.New()

func readJSONRow(rows *sqlx.Rows, destination interface{}) error {
	var jsonBlob string
	err := rows.Scan(&jsonBlob)
	if err != nil {
		return errors.Wrap(err, "error scanning row json")
	}

	err = json.Unmarshal([]byte(jsonBlob), destination)
	if err != nil {
		return errors.Wrap(err, "error unmarshalling row json")
	}

	// validate our final struct
	err = validate.Struct(destination)
	if err != nil {
		return errors.Wrapf(err, "failed validation for json: %s", jsonBlob)
	}

	return nil
}

// extractValues is just a simple utility method that extracts the portion between `VALUE(`
// and `)` in the passed in string. (leaving VALUE but not the parentheses)
func extractValues(sql string) (string, error) {
	startValues := strings.Index(sql, "VALUES(")
	if startValues <= 0 {
		return "", errors.Errorf("unable to find VALUES( in bulk insert SQL: %s", sql)
	}

	// find the matching end parentheses, we need to count balances here
	openCount := 1
	endValues := -1
	for i, r := range sql[startValues+7:] {
		if r == '(' {
			openCount++
		} else if r == ')' {
			openCount--
			if openCount == 0 {
				endValues = i + startValues + 7
				break
			}
		}
	}

	if endValues <= 0 {
		return "", errors.Errorf("unable to find end of VALUES() in bulk insert sql: %s", sql)
	}

	return sql[startValues+6 : endValues+1], nil
}

type Queryer interface {
	Rebind(query string) string
	QueryxContext(ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error)
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	NamedExecContext(ctx context.Context, query string, arg interface{}) (sql.Result, error)
	GetContext(ctx context.Context, value interface{}, query string, args ...interface{}) error
}

// Exec calls ExecContext on the passed in Queryer, logging time taken if any rows were affected
func Exec(ctx context.Context, label string, tx Queryer, sql string, args ...interface{}) error {
	start := time.Now()
	res, err := tx.ExecContext(ctx, sql, args...)
	if err != nil {
		return errors.Wrapf(err, fmt.Sprintf("error %s", label))
	}
	rows, _ := res.RowsAffected()
	if rows > 0 {
		logrus.WithField("count", rows).WithField("elapsed", time.Since(start)).Debug(label)
	}
	return nil
}

// BulkSQL runs the SQL passed in for the passed in interfaces, replacing any variables in the SQL as needed
func BulkSQL(ctx context.Context, label string, tx Queryer, sql string, vs []interface{}) error {
	// no values, nothing to do
	if len(vs) == 0 {
		return nil
	}

	start := time.Now()

	// this will be our SQL placeholders for values in our final query, built dynamically
	values := strings.Builder{}
	values.Grow(7 * len(vs))

	// this will be each of the arguments to match the positional values above
	args := make([]interface{}, 0, len(vs)*5)

	// for each value we build a bound SQL statement, then extract the values clause
	for i, value := range vs {
		valueSQL, valueArgs, err := sqlx.Named(sql, value)
		if err != nil {
			return errors.Wrapf(err, "error converting bulk insert args")
		}

		args = append(args, valueArgs...)
		argValues, err := extractValues(valueSQL)
		if err != nil {
			return errors.Wrapf(err, "error extracting values from sql: %s", valueSQL)
		}

		// append to our global values, adding comma if necessary
		values.WriteString(argValues)
		if i+1 < len(vs) {
			values.WriteString(",")
		}
	}

	valuesSQL, err := extractValues(sql)
	if err != nil {
		return errors.Wrapf(err, "error extracting values from sql: %s", sql)
	}

	bulkQuery := tx.Rebind(strings.Replace(sql, valuesSQL, values.String(), -1))
	rows, err := tx.QueryxContext(ctx, bulkQuery, args...)
	if err != nil {
		return errors.Wrapf(err, "error during bulk query")
	}
	defer rows.Close()

	// if have a returning clause, read them back and try to map them
	if strings.Contains(strings.ToUpper(sql), "RETURNING") {
		for _, v := range vs {
			if !rows.Next() {
				return errors.Errorf("did not receive expected number of rows on insert")
			}

			err = rows.StructScan(v)
			if err != nil {
				return errors.Wrap(err, "error scanning for insert id")
			}
		}
	}

	// iterate our remaining rows
	for rows.Next() {
	}

	// check for any error
	if rows.Err() != nil {
		return errors.Wrapf(rows.Err(), "error in row cursor")
	}

	logrus.WithField("elapsed", time.Since(start)).WithField("rows", len(vs)).Infof("%s bulk sql complete", label)

	return nil
}
