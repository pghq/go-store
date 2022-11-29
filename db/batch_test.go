package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBatch_Do(t *testing.T) {
	t.Parallel()

	t.Run("ok", func(t *testing.T) {
		batch := Batch{}
		batch.Do(nil, Optional(true))
		assert.NotEmpty(t, batch)
	})
}

func TestBatch_One(t *testing.T) {
	t.Parallel()

	t.Run("ok", func(t *testing.T) {
		batch := Batch{}
		batch.One(nil, nil, Optional(true))
		assert.NotEmpty(t, batch)
	})
}

func TestBatch_Add(t *testing.T) {
	t.Parallel()

	t.Run("ok", func(t *testing.T) {
		batch := Batch{}
		batch.Add("tests", "", func(item *BatchItem) {})
		assert.NotEmpty(t, batch)
	})
}

func TestBatch_Edit(t *testing.T) {
	t.Parallel()

	t.Run("ok", func(t *testing.T) {
		batch := Batch{}
		batch.Edit("tests", Sql(""), "", func(item *BatchItem) {})
		assert.NotEmpty(t, batch)
	})
}

func TestBatch_All(t *testing.T) {
	t.Parallel()

	t.Run("ok", func(t *testing.T) {
		batch := Batch{}
		batch.All(nil, nil, Optional(true))
		assert.NotEmpty(t, batch)
	})
}
