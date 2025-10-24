package drivers

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/lib/pq"
)

type PqDriver struct {
	db *sql.DB
}

func (d *PqDriver) Connect(ctx context.Context, connString string) error {
	db, err := sql.Open("postgres", connString)
	if err != nil {
		return err
	}
	if err := db.PingContext(ctx); err != nil {
		return err
	}
	d.db = db
	return nil
}

func (d *PqDriver) Query(ctx context.Context, query string, args ...interface{}) (QueryResult, error) {
	// Detect if this is a statement that should use Exec instead of Query
	queryUpper := strings.ToUpper(strings.TrimSpace(query))
	isExecStatement := strings.HasPrefix(queryUpper, "INSERT") ||
		strings.HasPrefix(queryUpper, "UPDATE") ||
		strings.HasPrefix(queryUpper, "DELETE") ||
		strings.HasPrefix(queryUpper, "CREATE") ||
		strings.HasPrefix(queryUpper, "ALTER") ||
		strings.HasPrefix(queryUpper, "DROP")

	// Check if statement has RETURNING clause
	hasReturning := strings.Contains(queryUpper, "RETURNING")

	// Use QueryContext for SELECT or statements with RETURNING
	if !isExecStatement || hasReturning {
		rows, err := d.db.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, err
		}
		return &PqQueryResult{rows: rows}, nil
	}

	// Use ExecContext for INSERT/UPDATE/DELETE/DDL without RETURNING
	result, err := d.db.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	affected, err := result.RowsAffected()
	if err != nil {
		// This should rarely happen, but log it if it does
		affected = 0
	}

	return &PqExecResult{affected: affected}, nil
}

func (d *PqDriver) Close(ctx context.Context) error {
	if d.db != nil {
		return d.db.Close()
	}
	return nil
}

func (d *PqDriver) DriverName() string {
	return DriverNamePq
}

// ParseArrayFloat8 converts a value to a pq.Array-wrapped float64 slice.
// The pq driver requires arrays to be wrapped with pq.Array() and does not
// support null values within arrays.
func (d *PqDriver) ParseArrayFloat8(value interface{}) (interface{}, error) {
	switch v := value.(type) {
	case []float64:
		// Already converted, wrap with pq.Array
		return pq.Array(v), nil
	case []interface{}:
		// Convert to []float64
		floatArr := make([]float64, len(v))
		for i, item := range v {
			if item == nil {
				return nil, fmt.Errorf("pq driver does not support null values in arrays")
			}
			switch val := item.(type) {
			case float64:
				floatArr[i] = val
			case int:
				floatArr[i] = float64(val)
			case string:
				f, err := strconv.ParseFloat(val, 64)
				if err != nil {
					return nil, fmt.Errorf("invalid float in array: %v", val)
				}
				floatArr[i] = f
			default:
				return nil, fmt.Errorf("unsupported type in array: %T", item)
			}
		}
		return pq.Array(floatArr), nil
	default:
		return nil, fmt.Errorf("unsupported array type: %T", value)
	}
}

// PqQueryResult wraps sql.Rows to implement QueryResult
type PqQueryResult struct {
	rows *sql.Rows
}

func (r *PqQueryResult) Next() bool {
	return r.rows.Next()
}

func (r *PqQueryResult) Values() ([]interface{}, error) {
	cols, err := r.rows.Columns()
	if err != nil {
		return nil, err
	}
	values := make([]interface{}, len(cols))
	valuePtrs := make([]interface{}, len(cols))
	for i := range values {
		valuePtrs[i] = &values[i]
	}
	if err := r.rows.Scan(valuePtrs...); err != nil {
		return nil, err
	}

	// Convert []byte to string for consistency with pgx
	for i, v := range values {
		if b, ok := v.([]byte); ok {
			values[i] = string(b)
		}
	}

	return values, nil
}

func (r *PqQueryResult) Err() error {
	return r.rows.Err()
}

func (r *PqQueryResult) Close() {
	r.rows.Close()
}

func (r *PqQueryResult) RowsAffected() int64 {
	return 0 // Not available for query results in database/sql
}

// PqExecResult represents the result of an Exec operation
type PqExecResult struct {
	affected int64
}

func (r *PqExecResult) Next() bool {
	return false
}

func (r *PqExecResult) Values() ([]interface{}, error) {
	return nil, nil
}

func (r *PqExecResult) Err() error {
	return nil
}

func (r *PqExecResult) Close() {
	// Nothing to close
}

func (r *PqExecResult) RowsAffected() int64 {
	return r.affected
}
