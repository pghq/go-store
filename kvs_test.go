package ark

import (
	"context"
	"testing"

	"github.com/pghq/go-tea"
	"github.com/stretchr/testify/assert"
)

func TestKVSConn_Txn(t *testing.T) {
	t.Parallel()

	conn, _ := Open().ConnectKVS(context.TODO(), "inmem")

	t.Run("context timeout", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.TODO(), 0)
		defer cancel()
		_, err := conn.Txn(ctx)
		assert.NotNil(t, err)
	})

	t.Run("new", func(t *testing.T) {
		tx, err := conn.Txn(context.TODO())
		assert.Nil(t, err)
		assert.NotNil(t, tx)
		assert.True(t, tx.root)
	})

	t.Run("fork", func(t *testing.T) {
		tx, _ := conn.Txn(context.TODO())
		tx, err := conn.Txn(tx)
		assert.Nil(t, err)
		assert.NotNil(t, tx)
		assert.False(t, tx.root)
	})
}

func TestKVSConn_Do(t *testing.T) {
	t.Parallel()

	conn, _ := Open().ConnectKVS(context.TODO(), "inmem")

	t.Run("context timeout", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.TODO(), 0)
		defer cancel()

		err := conn.Do(ctx, func(tx *KVSTxn) error { return nil })
		assert.NotNil(t, err)
	})

	t.Run("fn error", func(t *testing.T) {
		err := conn.Do(context.TODO(), func(tx *KVSTxn) error { return tea.NewError("fn error") })
		assert.NotNil(t, err)
	})

	t.Run("no error", func(t *testing.T) {
		err := conn.Do(context.TODO(), func(tx *KVSTxn) error { return nil })
		assert.Nil(t, err)
	})
}

func TestKVSTxn(t *testing.T) {
	t.Parallel()

	conn, _ := Open().ConnectKVS(context.TODO(), "inmem")

	t.Run("insert", func(t *testing.T) {
		tx, _ := conn.Txn(context.TODO())
		defer tx.Commit()
		_, err := tx.Insert([]byte("test"), "value").Resolve()
		assert.Nil(t, err)
	})

	t.Run("insert with ttl", func(t *testing.T) {
		tx, _ := conn.Txn(context.TODO())
		defer tx.Commit()
		_, err := tx.InsertWithTTL([]byte("test2"), "value", 0).Resolve()
		assert.Nil(t, err)
	})

	t.Run("get", func(t *testing.T) {
		tx, _ := conn.Txn(context.TODO())
		defer tx.Commit()
		var value string
		_, err := tx.Get([]byte("test"), &value).Resolve()
		assert.Nil(t, err)
	})

	t.Run("remove", func(t *testing.T) {
		tx, _ := conn.Txn(context.TODO())
		defer tx.Commit()
		_, err := tx.Remove([]byte("test")).Resolve()
		assert.Nil(t, err)
	})
}
