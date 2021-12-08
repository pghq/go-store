package ark

import (
	"context"
	"testing"

	"github.com/pghq/go-tea"
	"github.com/stretchr/testify/assert"

	"github.com/pghq/go-ark/rdb"
)

func TestRDBConn_Txn(t *testing.T) {
	t.Parallel()

	conn, _ := Open().
		DSN(rdb.Schema{
			"tests": map[string][]string{
				"primary": {"id"},
			},
		}).
		ConnectRDB(context.TODO(), "inmem")

	t.Run("context timeout", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.TODO(), 0)
		defer cancel()
		_, err := conn.Txn(ctx)
		assert.NotNil(t, err)
	})

	t.Run("new", func(t *testing.T) {
		tx, err := conn.Txn(context.TODO())
		defer tx.Rollback()
		assert.Nil(t, err)
		assert.NotNil(t, tx)
		assert.True(t, tx.root)
	})

	t.Run("fork", func(t *testing.T) {
		tx, _ := conn.Txn(context.TODO())
		tx, err := conn.Txn(tx)
		defer tx.Rollback()
		assert.Nil(t, err)
		assert.NotNil(t, tx)
		assert.False(t, tx.root)
	})
}

func TestRDBConn_Do(t *testing.T) {
	t.Parallel()

	conn, _ := Open().
		DSN(rdb.Schema{
			"tests": map[string][]string{
				"primary": {"id"},
			},
		}).
		ConnectRDB(context.TODO(), "inmem")

	t.Run("context timeout", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.TODO(), 0)
		defer cancel()

		err := conn.Do(ctx, func(tx *RDBTxn) error { return nil })
		assert.NotNil(t, err)
	})

	t.Run("fn error", func(t *testing.T) {
		err := conn.Do(context.TODO(), func(tx *RDBTxn) error { return tea.NewError("fn error") })
		assert.NotNil(t, err)
	})

	t.Run("no error", func(t *testing.T) {
		err := conn.Do(context.TODO(), func(tx *RDBTxn) error { return nil })
		assert.Nil(t, err)
	})
}

func TestRDBTxn(t *testing.T) {
	t.Parallel()

	conn, _ := Open().
		DSN(rdb.Schema{
			"tests": map[string][]string{
				"primary": {"id"},
			},
		}).
		ConnectRDB(context.TODO(), "inmem")

	type data struct {
		Id string `db:"id"`
	}

	t.Run("insert", func(t *testing.T) {
		tx, _ := conn.Txn(context.TODO())
		defer tx.Commit()
		_, err := tx.Insert("tests", data{Id: "test"}).Resolve()
		assert.Nil(t, err)
	})

	t.Run("update", func(t *testing.T) {
		tx, _ := conn.Txn(context.TODO())
		defer tx.Commit()
		_, err := tx.Update("tests", rdb.Ft().IdxEq("primary", "test"), data{Id: "test"}).Resolve()
		assert.Nil(t, err)
	})

	t.Run("get", func(t *testing.T) {
		tx, _ := conn.Txn(context.TODO())
		defer tx.Commit()
		var value data
		_, err := tx.Get(Qy().Table("tests").Filter(rdb.Ft().IdxEq("primary", "test")), &value).Resolve()
		assert.Nil(t, err)
	})

	t.Run("list", func(t *testing.T) {
		tx, _ := conn.Txn(context.TODO())
		defer tx.Commit()
		var values []data
		_, err := tx.List(Qy().Table("tests").Filter(rdb.Ft().IdxEq("primary", "test")), &values).Resolve()
		assert.NotNil(t, err)
	})

	t.Run("remove", func(t *testing.T) {
		tx, _ := conn.Txn(context.TODO())
		defer tx.Commit()
		_, err := tx.Remove("tests", rdb.Ft().IdxEq("primary", "test")).Resolve()
		assert.Nil(t, err)
	})
}
