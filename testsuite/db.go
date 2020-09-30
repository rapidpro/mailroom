package testsuite

import (
	"context"
	"database/sql"

	"github.com/jmoiron/sqlx"
)

type MockDB struct {
	real       *sqlx.DB
	callCounts map[string]int
	shouldErr  func(funcName string, call int) error
}

func NewMockDB(db *sqlx.DB, shouldErr func(funcName string, call int) error) *MockDB {
	return &MockDB{
		real:       db,
		callCounts: make(map[string]int),
		shouldErr:  shouldErr,
	}
}

func (d *MockDB) check(funcName string) error {
	call := d.callCounts[funcName]
	d.callCounts[funcName]++
	return d.shouldErr(funcName, call)
}

func (d *MockDB) Rebind(query string) string {
	return d.Rebind(query)
}

func (d *MockDB) QueryxContext(ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error) {
	if err := d.check("QueryxContext"); err != nil {
		return nil, err
	}
	return d.real.QueryxContext(ctx, query, args...)
}

func (d *MockDB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	if err := d.check("ExecContext"); err != nil {
		return nil, err
	}
	return d.real.ExecContext(ctx, query, args...)
}

func (d *MockDB) NamedExecContext(ctx context.Context, query string, arg interface{}) (sql.Result, error) {
	if err := d.check("NamedExecContext"); err != nil {
		return nil, err
	}
	return d.real.NamedExecContext(ctx, query, arg)
}

func (d *MockDB) GetContext(ctx context.Context, value interface{}, query string, args ...interface{}) error {
	if err := d.check("GetContext"); err != nil {
		return err
	}
	return d.real.GetContext(ctx, value, query, args...)
}

func (d *MockDB) BeginTxx(ctx context.Context, opts *sql.TxOptions) (*sqlx.Tx, error) {
	if err := d.check("BeginTxx"); err != nil {
		return nil, err
	}
	return d.real.BeginTxx(ctx, opts)
}
