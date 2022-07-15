package internal

import (
	"github.com/jackc/pgconn"
	"github.com/pghq/go-tea/trail"
)

const (
	// ErrCodeUniqueViolation expected pg error code for unique violations
	ErrCodeUniqueViolation = "23505"
)

// IsErrorCode checks if error code matches underlying pg code
func IsErrorCode(err error, code string) bool {
	var icv *pgconn.PgError
	return err != nil && trail.AsError(err, &icv) && code == icv.Code
}
