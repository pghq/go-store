package redis

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/pghq/go-tea"
	"github.com/stretchr/testify/assert"

	"github.com/pghq/go-ark/internal"
)

func TestProvider_Connect(t *testing.T) {
	t.Parallel()

	t.Run("context timeout", func(t *testing.T) {
		p := NewProvider("", Config{})
		ctx, cancel := context.WithTimeout(context.TODO(), 0)
		defer cancel()
		err := p.Connect(ctx)
		assert.NotNil(t, err)
	})
}

func TestProvider_Txn(t *testing.T) {
	t.Parallel()

	s, _ := miniredis.Run()
	defer s.Close()

	p := NewProvider(s.Addr(), Config{})
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
		res := txn.Exec(internal.Insert{Value: func() {}})
		delta, err := res.Resolve()
		assert.NotNil(t, err)
		assert.Equal(t, 0, delta)
	})

	t.Run("bad value", func(t *testing.T) {
		txn, _ := p.Txn(context.TODO())
		defer txn.Rollback()
		res := txn.Exec(internal.Insert{Key: []byte("test"), Value: func() {}})
		delta, err := res.Resolve()
		assert.NotNil(t, err)
		assert.Equal(t, 0, delta)
	})

	t.Run("bad op", func(t *testing.T) {
		txn, _ := p.Txn(context.TODO())
		defer txn.Rollback()
		res := txn.Exec(internal.List{}, &map[string]interface{}{})
		delta, err := res.Resolve()
		assert.NotNil(t, err)
		assert.Equal(t, 0, delta)
	})

	t.Run("missing destination", func(t *testing.T) {
		txn, _ := p.Txn(context.TODO())
		defer txn.Rollback()
		res := txn.Exec(internal.Get{Key: []byte("test")})
		delta, err := res.Resolve()
		assert.NotNil(t, err)
		assert.Equal(t, 0, delta)
	})

	t.Run("bad destination", func(t *testing.T) {
		txn, _ := p.Txn(context.TODO())
		defer txn.Rollback()

		var value int
		_ = txn.Exec(internal.Insert{Key: []byte("test"), Value: "value"})
		res := txn.Exec(internal.Get{Key: []byte("test")}, &value)

		err := txn.Commit()
		assert.Nil(t, err)

		delta, err := res.Resolve()
		assert.NotNil(t, err)
		assert.Equal(t, 0, delta)
	})

	t.Run("not found", func(t *testing.T) {
		txn, _ := p.Txn(context.TODO())
		var value string
		_ = txn.Exec(internal.Get{Key: []byte("not found")}, &value)
		err := txn.Commit()
		assert.NotNil(t, err)
		assert.False(t, tea.IsFatal(err))
	})

	t.Run("read only", func(t *testing.T) {
		txn, _ := p.Txn(context.TODO(), true)
		var value string
		_ = txn.Exec(internal.Get{Key: []byte("not found")}, &value)
		err := txn.Commit()
		assert.NotNil(t, err)
		assert.False(t, tea.IsFatal(err))
	})

	t.Run("success", func(t *testing.T) {
		txn, _ := p.Txn(context.TODO())
		defer txn.Rollback()

		var (
			value1 string
			value2 string
		)

		resolvers := []internal.Resolver{
			txn.Exec(internal.Insert{Key: []byte("test"), Value: "value"}),
			txn.Exec(internal.Update{Key: []byte("test"), Value: "value"}),
			txn.Exec(internal.Get{Key: []byte("test")}, &value1),
			txn.Exec(internal.Insert{Key: []byte("another"), Value: "money"}),
			txn.Exec(internal.Get{Key: []byte("another")}, &value2),
			txn.Exec(internal.Insert{Key: []byte("ttl"), Value: "money"}, time.Duration(0)),
			txn.Exec(internal.Remove{Key: []byte("another")}),
		}

		err := txn.Commit()
		assert.Nil(t, err)

		for _, res := range resolvers {
			delta, err := res.Resolve()
			assert.Nil(t, err)
			assert.Equal(t, 1, delta)
		}

		assert.Equal(t, "value", value1)
		assert.Equal(t, "money", value2)
	})
}
