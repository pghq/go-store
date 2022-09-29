package pgtest

import (
	"testing"

	"github.com/pghq/go-tea/trail"
	"github.com/stretchr/testify/assert"
)

func TestStart(t *testing.T) {
	trail.Testing()
	t.Parallel()

	t.Run("ok", func(t *testing.T) {
		dsn, cleanup, err := Start()
		assert.Nil(t, err)
		assert.NotEmpty(t, dsn)
		assert.NotNil(t, cleanup)
		cleanup()
	})
}
