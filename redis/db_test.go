package redis

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"

	"github.com/pghq/go-ark/db"
)

func TestNewDB(t *testing.T) {
	t.Parallel()

	t.Run("bad open", func(t *testing.T) {
		d := NewDB()
		assert.NotNil(t, d.Ping(context.TODO()))
	})

	s, _ := miniredis.Run()
	defer s.Close()

	t.Run("with custom dsn", func(t *testing.T) {
		d := NewDB(db.DSN(s.Addr()), db.Redis(redis.Options{}))
		assert.NotNil(t, d)
		assert.Nil(t, d.Ping(context.TODO()))
	})
}

func TestDB_Txn(t *testing.T) {
	t.Parallel()

	s, _ := miniredis.Run()
	defer s.Close()

	d := NewDB(db.DSN(s.Addr()))

	t.Run("write", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		assert.NotNil(t, tx)
	})

	t.Run("read only", func(t *testing.T) {
		tx := d.Txn(context.TODO(), db.ReadOnly())
		assert.NotNil(t, tx)
	})

	t.Run("can rollback", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		err := tx.Rollback()
		assert.Nil(t, err)
	})

	t.Run("can commit", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		assert.NotNil(t, tx)
		err := tx.Commit()
		assert.Nil(t, err)
	})
}

func TestTxn_Insert(t *testing.T) {
	t.Parallel()

	s, _ := miniredis.Run()
	defer s.Close()

	d := NewDB(db.DSN(s.Addr()))

	t.Run("bad value", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Insert("tests", db.Id("foo"), func() {})
		assert.NotNil(t, err)
	})

	t.Run("ok", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Insert("tests", db.Id("foo"), map[string]interface{}{"id": "foo"})
		assert.Nil(t, err)
		assert.Nil(t, tx.Commit())
	})
}

func TestTxn_Update(t *testing.T) {
	t.Parallel()

	s, _ := miniredis.Run()
	defer s.Close()

	d := NewDB(db.DSN(s.Addr()))

	t.Run("not found", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Update("tests", db.Id("foo"), map[string]interface{}{"id": "foo"})
		assert.NotNil(t, err)
	})

	t.Run("ok", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		tx.Insert("tests", db.Id("foo"), map[string]interface{}{"id": "foo"})
		tx.Commit()

		tx = d.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Update("tests", db.Id("foo"), map[string]interface{}{"id": "foo"})
		assert.Nil(t, err)
	})
}

func TestTxn_Remove(t *testing.T) {
	t.Parallel()

	s, _ := miniredis.Run()
	defer s.Close()

	d := NewDB(db.DSN(s.Addr()))

	t.Run("ok", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		tx.Insert("tests", db.Id("foo"), map[string]interface{}{"id": "foo"})
		tx.Commit()

		tx = d.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Remove("tests", db.Id("foo"))
		assert.Nil(t, tx.Commit())

		tx = d.Txn(context.TODO())
		defer tx.Rollback()
		err = tx.Update("tests", db.Id("foo"), map[string]interface{}{"id": "foo"})
		assert.NotNil(t, err)
	})
}

func TestTxn_Get(t *testing.T) {
	t.Parallel()

	s, _ := miniredis.Run()
	defer s.Close()

	d := NewDB(db.DSN(s.Addr()))
	tx := d.Txn(context.TODO())
	tx.Insert("tests", db.Id("foo"), map[string]interface{}{"id": "foo"})
	tx.Insert("units", db.Id("foo"), map[string]interface{}{"id": "foo"})
	tx.Commit()

	t.Run("read batch size exhausted", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		var v map[string]interface{}
		tx.Get("tests", db.Id("foo"), &v)
		err := tx.Get("tests", db.Id("foo"), &v)
		assert.NotNil(t, err)
	})

	t.Run("not found", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		var v map[string]interface{}
		tx.Get("tests", db.Id("not found"), &v)
		assert.NotNil(t, tx.Commit())
	})

	t.Run("rolled back", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		var v map[string]interface{}
		tx.Get("tests", db.Id("not found"), &v)
		tx.Rollback()
		assert.NotNil(t, tx.Commit())
	})

	t.Run("bad decode value", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		var v func()
		tx.Get("tests", db.Id("foo"), &v)
		assert.NotNil(t, tx.Commit())
	})

	t.Run("ok", func(t *testing.T) {
		tx := d.Txn(context.TODO(), db.BatchReadSize(2))
		var v1 map[string]interface{}
		var v2 map[string]interface{}
		err := tx.Get("tests", db.Id("foo"), &v1)
		assert.Nil(t, err)

		err = tx.Get("units", db.Id("foo"), &v2)
		assert.Nil(t, err)

		assert.Nil(t, tx.Commit())
		assert.Equal(t, map[string]interface{}{"id": "foo"}, v1)
		assert.Equal(t, map[string]interface{}{"id": "foo"}, v2)
	})
}

func TestTxn_List(t *testing.T) {
	t.Parallel()

	s, _ := miniredis.Run()
	defer s.Close()

	d := NewDB(db.DSN(s.Addr()))
	tx := d.Txn(context.TODO())
	tx.Insert("tests", db.Id("foo"), map[string]interface{}{"id": "foo"})
	tx.Insert("tests", db.Id("bar"), map[string]interface{}{"id": "bar"})
	tx.Insert("units", db.Id("foo"), map[string]interface{}{"id": "foo"})
	tx.Insert("units", db.Id("bar"), map[string]interface{}{"id": "bar"})
	tx.Commit()

	t.Run("dst must be a pointer", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		var v map[string]interface{}
		err := tx.List("tests", v)
		assert.NotNil(t, err)
	})

	t.Run("dst must be a pointer to slice", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		var v map[string]interface{}
		err := tx.List("tests", &v)
		assert.NotNil(t, err)
	})

	t.Run("read batch size exhausted", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		var v []map[string]interface{}
		tx.List("tests", &v)
		err := tx.List("tests", &v)
		assert.NotNil(t, err)
	})

	t.Run("not found", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		var v []map[string]interface{}
		tx.List("tests", &v, db.Page(10))
		assert.NotNil(t, tx.Commit())
	})

	t.Run("not found limit", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		var v []map[string]interface{}
		tx.List("tests", &v, db.Page(10), db.Limit(1))
		assert.NotNil(t, tx.Commit())
	})

	t.Run("bad decode value", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		var v []func()
		tx.List("tests", &v)
		assert.NotNil(t, tx.Commit())
	})

	t.Run("ok", func(t *testing.T) {
		tx := d.Txn(context.TODO(), db.BatchReadSize(2))
		var v1 []map[string]interface{}
		var v2 []map[string]interface{}
		err := tx.List("tests", &v1)
		assert.Nil(t, err)

		err = tx.List("units", &v2)
		assert.Nil(t, err)

		assert.Nil(t, tx.Commit())
		assert.Len(t, v1, 2)
		assert.Len(t, v1, 2)
	})
}
