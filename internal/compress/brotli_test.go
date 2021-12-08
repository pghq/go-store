package compress

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBrotliEncode(t *testing.T) {
	t.Run("bad value", func(t *testing.T) {
		_, err := BrotliEncode(func() {})
		assert.NotNil(t, err)
	})

	t.Run("can encode", func(t *testing.T) {
		b, err := BrotliEncode("a really long run-on sentence, a really long run-on sentence")
		assert.Nil(t, err)
		assert.NotNil(t, b)
	})
}

func TestBrotliHash(t *testing.T) {
	t.Run("bad value", func(t *testing.T) {
		_, err := BrotliHash(func() {})
		assert.NotNil(t, err)
	})

	t.Run("can encode", func(t *testing.T) {
		b, err := BrotliHash("a really long run-on sentence, a really long run-on sentence")
		assert.Nil(t, err)
		assert.NotNil(t, b)
	})
}

func TestBrotliDecode(t *testing.T) {
	t.Run("can decode", func(t *testing.T) {
		var v string
		b, _ := BrotliEncode("foo")
		err := BrotliDecode(b, &v)
		assert.Nil(t, err)
		assert.Equal(t, "foo", v)
	})
}
