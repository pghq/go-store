package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMap(t *testing.T) {
	t.Parallel()

	t.Run("map", func(t *testing.T) {
		m, _ := Map(map[string]interface{}{"id": "foo"})
		assert.Equal(t, map[string]interface{}{"id": "foo"}, m)
	})

	t.Run("unrecognized type", func(t *testing.T) {
		_, err := Map(func() {})
		assert.NotNil(t, err)
	})

	t.Run("struct pointer", func(t *testing.T) {
		type value struct {
			Field1 int `db:"field1"`
			Field2 int `db:"field2,transient"`
			Field3 int `db:"-"`
			Field4 int
		}

		v := value{
			Field1: 1,
			Field2: 2,
			Field3: 3,
			Field4: 4,
		}

		m, _ := Map(&v, true)
		assert.Equal(t, map[string]interface{}{"field1": 1, "field2": 2, "Field4": 4}, m)
	})
}

func TestCopy(t *testing.T) {
	t.Run("can not set", func(t *testing.T) {
		err := Copy(func() {}, "")
		assert.NotNil(t, err)
	})

	t.Run("ok", func(t *testing.T) {
		var v string
		assert.Nil(t, Copy("foo", &v))
		assert.Equal(t, "foo", v)
	})
}
