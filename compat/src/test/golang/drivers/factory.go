package drivers

import (
	"fmt"
)

// NewDriver creates a new DatabaseDriver instance based on the driver name.
// Use DriverNamePgx or DriverNamePq constants.
func NewDriver(driverName string) (DatabaseDriver, error) {
	switch driverName {
	case DriverNamePgx:
		return &PgxDriver{}, nil
	case DriverNamePq:
		return &PqDriver{}, nil
	default:
		return nil, fmt.Errorf("unsupported driver: %s (supported: %s, %s)",
			driverName, DriverNamePgx, DriverNamePq)
	}
}

// SupportedDrivers returns a list of all supported driver names
func SupportedDrivers() []string {
	return []string{DriverNamePgx, DriverNamePq}
}
