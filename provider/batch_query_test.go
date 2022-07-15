package provider

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBatchQuery_One(t *testing.T) {
	t.Parallel()

	t.Run("ok", func(t *testing.T) {
		batch := BatchQuery{}
		batch.One(nil, nil)
		assert.NotEmpty(t, batch)
	})
}

func TestBatchQuery_All(t *testing.T) {
	t.Parallel()

	t.Run("ok", func(t *testing.T) {
		batch := BatchQuery{}
		batch.All(nil, nil)
		assert.NotEmpty(t, batch)
	})
}
