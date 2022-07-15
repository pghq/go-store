package internal

import (
	"testing"

	"github.com/jackc/pgconn"
	"github.com/stretchr/testify/assert"
)

func TestIsErrorCode(t *testing.T) {
	t.Parallel()

	t.Run("unique violation", func(t *testing.T) {
		assert.True(t, IsErrorCode(&pgconn.PgError{Code: ErrCodeUniqueViolation}, ErrCodeUniqueViolation))
	})
}
