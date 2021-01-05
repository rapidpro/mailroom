package dbutil

import (
	"context"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

// Queryer is the DB/TX functionality needed for operations in this package
type Queryer interface {
	Rebind(query string) string
	QueryxContext(ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error)
}

// BulkQuery runs the query as a bulk operation with the given structs
func BulkQuery(ctx context.Context, tx Queryer, query string, structs []interface{}) error {
	// no structs, nothing to do
	if len(structs) == 0 {
		return nil
	}

	// rewrite query as a bulk operation
	bulkQuery, args, err := BulkSQL(tx, query, structs)
	if err != nil {
		return err
	}

	rows, err := tx.QueryxContext(ctx, bulkQuery, args...)
	if err != nil {
		return errors.Wrapf(err, "error making bulk query: %.100s", bulkQuery)
	}
	defer rows.Close()

	// if have a returning clause, read them back and try to map them
	if strings.Contains(strings.ToUpper(query), "RETURNING") {
		for _, s := range structs {
			if !rows.Next() {
				return errors.Errorf("did not receive expected number of rows on insert")
			}

			err = rows.StructScan(s)
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

	return nil
}

// BulkSQL takes a query which uses VALUES with struct bindings and rewrites it as a bulk operation.
// It returns the new SQL query and the args to pass to it.
func BulkSQL(tx Queryer, sql string, structs []interface{}) (string, []interface{}, error) {
	if len(structs) == 0 {
		return "", nil, errors.New("can't generate bulk sql with zero structs")
	}

	// this will be our SQL placeholders for values in our final query, built dynamically
	values := strings.Builder{}
	values.Grow(7 * len(structs))

	// this will be each of the arguments to match the positional values above
	args := make([]interface{}, 0, len(structs)*5)

	// for each value we build a bound SQL statement, then extract the values clause
	for i, value := range structs {
		valueSQL, valueArgs, err := sqlx.Named(sql, value)
		if err != nil {
			return "", nil, errors.Wrapf(err, "error converting bulk insert args")
		}

		args = append(args, valueArgs...)
		argValues := extractValues(valueSQL)
		if argValues == "" {
			return "", nil, errors.Errorf("error extracting VALUES from sql: %s", valueSQL)
		}

		// append to our global values, adding comma if necessary
		values.WriteString(argValues)
		if i+1 < len(structs) {
			values.WriteString(",")
		}
	}

	valuesSQL := extractValues(sql)
	if valuesSQL == "" {
		return "", nil, errors.Errorf("error extracting VALUES from sql: %s", sql)
	}

	return tx.Rebind(strings.Replace(sql, valuesSQL, values.String(), -1)), args, nil
}

// extractValues is just a simple utility method that extracts the portion between `VALUE(`
// and `)` in the passed in string. (leaving VALUE but not the parentheses)
func extractValues(sql string) string {
	startValues := strings.Index(sql, "VALUES(")
	if startValues <= 0 {
		return ""
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
		return ""
	}

	return sql[startValues+6 : endValues+1]
}
