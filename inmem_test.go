package ark

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInMemoryStore_Insert(t *testing.T) {
	s := NewInMemory()

	t.Run("bad value", func(t *testing.T) {
		_, err := s.Insert("test", func() {})
		assert.NotNil(t, err)
	})

	t.Run("success", func(t *testing.T) {
		item, err := s.Insert("test", "value")
		assert.Nil(t, err)
		assert.NotNil(t, item)

		item, err = s.Insert("test", "value")
		assert.Nil(t, err)
		assert.NotNil(t, item)

		item, err = s.Get(item.Id)
		assert.Nil(t, err)
		assert.NotNil(t, item)

		item, err = s.Get("test")
		assert.Nil(t, err)
		assert.NotNil(t, item)

		var value string
		err = item.Value(&value)
		assert.Nil(t, err)
		assert.Equal(t, "value", value)

		another, err := s.Insert("another", "value")
		assert.Nil(t, err)
		assert.NotNil(t, another)

		another, err = s.Get(another.Id)
		assert.Nil(t, err)
		assert.NotNil(t, another)

		assert.NotNil(t, item.Id, another.Id)
	})
}

func TestInMemoryStore_Get(t *testing.T) {
	s := NewInMemory()

	t.Run("not found int", func(t *testing.T) {
		_, err := s.Get(0)
		assert.NotNil(t, err)
	})

	t.Run("not found string", func(t *testing.T) {
		_, err := s.Get("key")
		assert.NotNil(t, err)
	})

	t.Run("bad key", func(t *testing.T) {
		_, err := s.Get(0.0)
		assert.NotNil(t, err)
	})
}
