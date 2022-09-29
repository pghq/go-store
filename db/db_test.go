package db

import (
	"testing"

	"github.com/Masterminds/squirrel"
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

func TestNewSpec(t *testing.T) {
	trail.Testing()
	t.Parallel()

	t.Run("ok", func(t *testing.T) {
		spec := NewSpec("spec", squirrel.Expr("SELECT * from specs"))
		assert.NotNil(t, spec)
		assert.Equal(t, "spec", spec.Id())
		_, _, err := spec.ToSql()
		assert.Nil(t, err)

		def := DeferSpec(func() Spec { return spec })
		assert.NotNil(t, def)
		assert.Equal(t, "spec", def.Id())
		_, _, err = def.ToSql()
		assert.Nil(t, err)
	})
}
