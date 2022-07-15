package provider

import (
	"testing"

	"github.com/pghq/go-tea/trail"
	"github.com/stretchr/testify/assert"
)

func TestWithReadOnly(t *testing.T) {
	trail.Testing()
	t.Parallel()

	t.Run("ok", func(t *testing.T) {
		conf := TxConfig{}
		WithReadOnly(true)(&conf)
		assert.True(t, conf.ReadOnly)
	})
}
