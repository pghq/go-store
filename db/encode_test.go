package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncode(t *testing.T) {
	t.Run("bad value", func(t *testing.T) {
		_, err := Encode(func() {})
		assert.NotNil(t, err)
	})

	t.Run("interface slice", func(t *testing.T) {
		b, err := Encode([]interface{}{"foo", nil})
		assert.Nil(t, err)
		assert.NotNil(t, b)
	})

	t.Run("string", func(t *testing.T) {
		b, err := Encode("a really long run-on sentence, a really long run-on sentence")
		assert.Nil(t, err)
		assert.NotNil(t, b)
	})
}

func TestDecode(t *testing.T) {
	t.Run("can decode", func(t *testing.T) {
		var v string
		b, _ := Encode("foo")
		err := Decode(b, &v)
		assert.Nil(t, err)
		assert.Equal(t, "foo", v)
	})
}
