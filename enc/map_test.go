package enc

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMap(t *testing.T) {
	t.Parallel()

	t.Run("map", func(t *testing.T) {
		m := Map(map[string]interface{}{"id": "foo"})
		assert.Equal(t, map[string]interface{}{"id": "foo"}, m)
	})

	t.Run("map pointer", func(t *testing.T) {
		m := Map(&map[string]interface{}{"id": "foo"})
		assert.Equal(t, map[string]interface{}{"id": "foo"}, m)
	})

	t.Run("pointer to map pointer", func(t *testing.T) {
		pm := &map[string]interface{}{"id": "foo"}
		m := Map(&pm)
		assert.Nil(t, m)
	})

	t.Run("unrecognized type", func(t *testing.T) {
		m := Map(func() {})
		assert.Nil(t, m)
	})

	t.Run("struct pointer", func(t *testing.T) {
		type value struct {
			Field1    int `db:"field1"`
			Field2    int `db:"field2,transient"`
			Field3    int `db:"-"`
			FieldFour int
			Ignore    func()
		}

		v := value{
			Field1:    1,
			Field2:    2,
			Field3:    3,
			FieldFour: 4,
		}

		m := Map(&v, "ignore")
		assert.Equal(t, map[string]interface{}{"field1": 1, "field2": 2, "field_four": 4}, m)
	})

	t.Run("struct slice", func(t *testing.T) {
		type value struct {
			Field1 int `db:"field1"`
			Field2 int `db:"field2,transient"`
			Field3 int `db:"-"`
			Field4 int
		}

		v := []value{{
			Field1: 1,
			Field2: 2,
			Field3: 3,
			Field4: 4,
		}}

		m := Map(&v)
		assert.Equal(t, map[string]interface{}{"field1": 0, "field2": 0, "field4": 0}, m)
	})
}
