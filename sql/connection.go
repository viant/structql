package sql

import (
	"context"
	"database/sql/driver"
	"fmt"
	"github.com/viant/afs"
	sqlparser "github.com/viant/sqlparser"
	"github.com/viant/x"
)

// Connection represent connection
type Connection struct {
	cfg   *Config
	types *x.Registry
	fs    afs.Service
}

// Prepare returns a prepared statement, bound to this Connection.
func (c *Connection) Prepare(SQL string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), SQL)
}

// PrepareContext returns a prepared statement, bound to this Connection.
func (c *Connection) PrepareContext(ctx context.Context, SQL string) (driver.Stmt, error) {
	kind := sqlparser.ParseKind(SQL)
	if !(kind.IsRegisterType() || kind.IsSelect()) {
		return nil, fmt.Errorf("unsupported SQL kind: %v", SQL)
	}
	c.types.Merge(globalTypes)
	stmt := &Statement{SQL: SQL, Kind: kind, types: c.types, BaseURL: c.cfg.BaseURL, fs: c.fs}
	stmt.checkQueryParameters()
	if kind.IsSelect() {
		if err := stmt.prepareSelect(SQL); err != nil {
			return nil, err
		}
	}
	return stmt, nil
}

// Ping pings server
func (c *Connection) Ping(ctx context.Context) error {
	return nil
}

// Begin starts and returns a new transaction.
func (c *Connection) Begin() (driver.Tx, error) {
	return nil, nil
}

// BeginTx starts and returns a new transaction.
func (c *Connection) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return nil, nil
}

// Close closes Connection
func (c *Connection) Close() error {
	return nil
}

// ResetSession resets session
func (c *Connection) ResetSession(ctx context.Context) error {
	return nil
}

// IsValid check is Connection is valid
func (c *Connection) IsValid() bool {
	return true
}
