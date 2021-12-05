package internal

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestToSnakeCase(t *testing.T) {
	t.Parallel()

	t.Run("from camel case", func(t *testing.T) {
		assert.Equal(t, "foo_bar", ToSnakeCase("fooBar"))
	})

	t.Run("from pascal case", func(t *testing.T) {
		assert.Equal(t, "foo_bar", ToSnakeCase("FooBar"))
	})
}

func TestFields(t *testing.T) {
	t.Parallel()

	t.Run("slice present", func(t *testing.T) {
		fields := Fields("field1", []string{"field2"})
		assert.Len(t, fields, 1)
		assert.Equal(t, []string{"field2"}, fields)
	})

	t.Run("mixed args", func(t *testing.T) {
		fields := Fields("field1", map[string]interface{}{"field3": ""})
		assert.Len(t, fields, 2)
		assert.Contains(t, fields, "field1")
		assert.Contains(t, fields, "field3")
	})

	t.Run("struct", func(t *testing.T) {
		var v struct {
			Field1 int `db:"field1"`
			Field2 int
		}
		fields := Fields(&v)
		assert.Len(t, fields, 2)
		assert.Contains(t, fields, "field1")
		assert.Contains(t, fields, "Field2")
	})

	t.Run("unknown type", func(t *testing.T) {
		var v int
		fields := Fields(&v)
		assert.Len(t, fields, 0)
	})
}
