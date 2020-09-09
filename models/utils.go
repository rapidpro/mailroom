package models

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/nyaruka/mailroom/utils/dbutil"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type Queryer interface {
	dbutil.Queryer

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

// BulkQuery runs the given query as a bulk operation
func BulkQuery(ctx context.Context, label string, tx Queryer, sql string, structs []interface{}) error {
	// no values, nothing to do
	if len(structs) == 0 {
		return nil
	}

	start := time.Now()

	err := dbutil.BulkQuery(ctx, tx, sql, structs)
	if err != nil {
		return errors.Wrap(err, "error making bulk query")
	}

	logrus.WithField("elapsed", time.Since(start)).WithField("rows", len(structs)).Infof("%s bulk sql complete", label)

	return nil
}
