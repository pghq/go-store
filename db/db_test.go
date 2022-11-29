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

func TestDo(t *testing.T) {
	trail.Testing()
	t.Parallel()

	t.Run("ok", func(t *testing.T) {
		deed := Sqlizer(squirrel.Expr("SELECT * from specs"))
		assert.NotNil(t, deed)
		assert.Equal(t, "SELECT * from specs []", deed.Id())
		_, _, err := deed.ToSql()
		assert.Nil(t, err)

		literal := Sql("SELECT * from specs")
		assert.NotNil(t, literal)
		assert.Equal(t, "SELECT * from specs []", literal.Id())
		_, _, err = literal.ToSql()
		assert.Nil(t, err)

		def := Defer(deed.Id(), func() squirrel.Sqlizer { return deed })
		assert.NotNil(t, def)
		assert.Equal(t, "SELECT * from specs []", def.Id())
		_, _, err = def.ToSql()
		assert.Nil(t, err)
	})
}
