package client

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKeys(t *testing.T) {
	t.Run("ignores nil snapper", func(t *testing.T) {
		keys := Keys(nil)
		assert.Nil(t, keys)
	})

	t.Run("can get keys", func(t *testing.T) {
		snapper := &snapper{
			Value: map[string]interface{}{
				"key1": "value1",
				"key2": "value2",
			},
		}

		keys := Keys(snapper)
		assert.Contains(t, keys, "key1")
		assert.Contains(t, keys,"key2")
	})
}

type snapper struct {
	Value map[string]interface{}
}

func (s *snapper) Snapshot() map[string]interface{}{
	return s.Value
}
