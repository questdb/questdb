package drivers

import (
	"context"
)

const (
	DriverNamePgx = "pgx"
	DriverNamePq  = "pq"
)

type DatabaseDriver interface {
	Connect(ctx context.Context, connString string) error
	Query(ctx context.Context, query string, args ...interface{}) (QueryResult, error)
	Close(ctx context.Context) error
	DriverName() string
	ParseArrayFloat8(value interface{}) (interface{}, error)
}

// QueryResult interface abstracts query result operations
type QueryResult interface {
	Next() bool
	Values() ([]interface{}, error)
	Err() error
	Close()
	RowsAffected() int64
}
