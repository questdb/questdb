package drivers

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// DBConnection, DBRows, CommandTag interfaces are assumed to be defined in this package or imported.

type PgxDriver struct {
	conn *pgx.Conn
}

func (d *PgxDriver) Connect(ctx context.Context, connString string) error {
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return err
	}
	d.conn = conn
	return nil
}

func (d *PgxDriver) Query(ctx context.Context, query string, args ...interface{}) (QueryResult, error) {
	rows, err := d.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return &PgxQueryResult{rows: rows}, nil
}

func (d *PgxDriver) Close(ctx context.Context) error {
	if d.conn != nil {
		return d.conn.Close(ctx)
	}
	return nil
}

func (d *PgxDriver) DriverName() string {
	return DriverNamePgx
}

// ParseArrayFloat8 converts a value to a format suitable for pgx.
// The pgx driver accepts []interface{} directly and supports null values.
func (d *PgxDriver) ParseArrayFloat8(value interface{}) (interface{}, error) {
	switch v := value.(type) {
	case []float64:
		// Convert to []interface{}
		result := make([]interface{}, len(v))
		for i, f := range v {
			result[i] = f
		}
		return result, nil
	case []interface{}:
		// Already in the right format, return as-is
		return v, nil
	default:
		return nil, fmt.Errorf("unsupported array type: %T", value)
	}
}

// PgxQueryResult wraps pgx.Rows to implement QueryResult
type PgxQueryResult struct {
	rows pgx.Rows
}

func (r *PgxQueryResult) Next() bool {
	return r.rows.Next()
}

func (r *PgxQueryResult) Values() ([]interface{}, error) {
	return r.rows.Values()
}

func (r *PgxQueryResult) Err() error {
	return r.rows.Err()
}

func (r *PgxQueryResult) Close() {
	r.rows.Close()
}

func (r *PgxQueryResult) RowsAffected() int64 {
	return r.rows.CommandTag().RowsAffected()
}
