package provider

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBatch_Do(t *testing.T) {
	t.Parallel()

	t.Run("ok", func(t *testing.T) {
		batch := Batch{}
		batch.Do(nil, WithBatchItemOptional(true))
		assert.NotEmpty(t, batch)
	})
}

func TestBatch_One(t *testing.T) {
	t.Parallel()

	t.Run("ok", func(t *testing.T) {
		batch := Batch{}
		batch.One(nil, nil, WithBatchItemOptional(true))
		assert.NotEmpty(t, batch)
	})
}

func TestBatch_All(t *testing.T) {
	t.Parallel()

	t.Run("ok", func(t *testing.T) {
		batch := Batch{}
		batch.All(nil, nil, WithBatchItemOptional(true))
		assert.NotEmpty(t, batch)
	})
}