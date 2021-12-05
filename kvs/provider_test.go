package kvs

import (
	"context"
	"testing"
	"time"

	"github.com/pghq/go-tea"
	"github.com/stretchr/testify/assert"

	"github.com/pghq/go-ark/internal"
)

func TestProvider_Txn(t *testing.T) {
	p := NewProvider()
	_ = p.Connect(context.TODO())

	t.Run("context timeout", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.TODO(), 0)
		defer cancel()

		_, err := p.Txn(ctx)
		assert.NotNil(t, err)
	})

	t.Run("bad key", func(t *testing.T) {
		txn, _ := p.Txn(context.TODO())
		defer txn.Rollback()
		ra, err := txn.Exec(internal.Insert{Value: func() {}}).Resolve()
		assert.NotNil(t, err)
		assert.Equal(t, 0, ra)
	})

	t.Run("bad value", func(t *testing.T) {
		txn, _ := p.Txn(context.TODO())
		defer txn.Rollback()
		ra, err := txn.Exec(internal.Insert{Key: []byte("test"), Value: func() {}}).Resolve()
		assert.NotNil(t, err)
		assert.Equal(t, 0, ra)

		ra, _ = txn.Exec(internal.Insert{Key: []byte("test"), Value: "value"}).Resolve()
		var values []int
		ra, err = txn.Exec(internal.List{}, &values).Resolve()
		assert.NotNil(t, err)

	})

	t.Run("bad op", func(t *testing.T) {
		txn, _ := p.Txn(context.TODO())
		ra, err := txn.Exec(internal.List{}, &map[string]interface{}{}).Resolve()
		assert.NotNil(t, err)
		assert.Equal(t, 0, ra)
	})

	t.Run("bad destination", func(t *testing.T) {
		txn, _ := p.Txn(context.TODO())
		defer txn.Rollback()
		ra, err := txn.Exec(internal.Get{Key: []byte("test")}).Resolve()
		assert.NotNil(t, err)
		assert.Equal(t, 0, ra)

		ra, err = txn.Exec(internal.List{}, []string{}).Resolve()
		assert.NotNil(t, err)
		assert.Equal(t, 0, ra)

		ra, err = txn.Exec(internal.List{}, &map[string]interface{}{}).Resolve()
		assert.NotNil(t, err)
		assert.Equal(t, 0, ra)
	})

	t.Run("not found", func(t *testing.T) {
		txn, _ := p.Txn(context.TODO())
		var value string
		ra, err := txn.Exec(internal.Get{Key: []byte("not found")}, &value).Resolve()
		assert.NotNil(t, err)
		assert.Equal(t, 0, ra)
	})

	t.Run("success", func(t *testing.T) {
		txn, _ := p.Txn(context.TODO())
		defer txn.Commit()

		ra, err := txn.Exec(internal.Insert{Key: []byte("test"), Value: "value"}).Resolve()
		assert.Nil(t, err)
		assert.Equal(t, 1, ra)

		ra, err = txn.Exec(internal.Update{Key: []byte("test"), Value: "value"}).Resolve()
		assert.Nil(t, err)
		assert.Equal(t, 1, ra)

		var value string
		ra, err = txn.Exec(internal.Get{Key: []byte("test")}, &value).Resolve()
		assert.Nil(t, err)
		assert.Equal(t, 1, ra)
		assert.Equal(t, "value", value)

		ra, err = txn.Exec(internal.Insert{Key: []byte("another"), Value: "money"}).Resolve()
		assert.Nil(t, err)
		assert.Equal(t, 1, ra)

		ra, err = txn.Exec(internal.Get{Key: []byte("another")}, &value).Resolve()
		assert.Nil(t, err)
		assert.Equal(t, 1, ra)
		assert.Equal(t, "money", value)

		ra, err = txn.Exec(internal.Insert{Key: []byte("ttl"), Value: "money"}, time.Duration(0)).Resolve()
		assert.Nil(t, err)
		assert.Equal(t, 1, ra)

		ra, err = txn.Exec(internal.Get{Key: []byte("ttl")}, &value).Resolve()
		assert.NotNil(t, err)
		assert.False(t, tea.IsFatal(err))

		ra, err = txn.Exec(internal.Remove{Key: []byte("another")}).Resolve()
		assert.Nil(t, err)
		assert.Equal(t, 1, ra)
	})
}
