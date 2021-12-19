package ark

import (
	"context"
	"testing"

	"github.com/pghq/go-tea"
	"github.com/stretchr/testify/assert"

	"github.com/pghq/go-ark/db"
	"github.com/pghq/go-ark/inmem"
)

func TestNew(t *testing.T) {
	t.Parallel()

	t.Run("bad open", func(t *testing.T) {
		m := NewSQL()
		assert.NotNil(t, m.Txn(context.TODO()).Commit())
		assert.NotNil(t, m.Txn(context.TODO()).Rollback())
		assert.NotNil(t, m.Txn(context.TODO()).Get("", "", nil))
		assert.NotNil(t, m.Txn(context.TODO()).Insert("", "", nil))
		assert.NotNil(t, m.Txn(context.TODO()).Remove("", ""))
		assert.NotNil(t, m.Txn(context.TODO()).Update("", "", nil))
		assert.NotNil(t, m.Txn(context.TODO()).List("", ""))
		assert.NotNil(t, m.Do(context.TODO(), func(tx db.Txn) error { return nil }))
		assert.NotNil(t, m.View(context.TODO(), func(tx db.Txn) error { return nil }))
	})

	t.Run("with error", func(t *testing.T) {
		m := New()
		m.SetError(tea.NewError("error"))
		assert.NotNil(t, m.Error())
	})

	t.Run("with default db", func(t *testing.T) {
		m := New()
		assert.NotNil(t, m)
		assert.Nil(t, m.err)
	})

	t.Run("with custom db", func(t *testing.T) {
		m := New(DB(inmem.NewDB()))
		assert.NotNil(t, m)
		assert.Nil(t, m.err)
	})

	t.Run("redis", func(t *testing.T) {
		m := NewRedis()
		assert.NotNil(t, m)
		assert.NotNil(t, m.err)
	})

	t.Run("rdb", func(t *testing.T) {
		m := NewRDB(db.Schema{})
		assert.NotNil(t, m)
		assert.Nil(t, m.err)
	})
}

func TestMapper_View(t *testing.T) {
	t.Parallel()

	m := New()
	t.Run("with fn error", func(t *testing.T) {
		err := m.View(context.TODO(), func(tx db.Txn) error { return tea.NewError("with fn error") })
		assert.NotNil(t, err)
	})

	t.Run("timeout", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.TODO(), 0)
		defer cancel()
		err := m.View(ctx, func(tx db.Txn) error { return nil })
		assert.NotNil(t, err)
	})

	t.Run("without fn error", func(t *testing.T) {
		err := m.View(context.TODO(), func(tx db.Txn) error { return nil })
		assert.Nil(t, err)
	})
}

func TestMapper_Do(t *testing.T) {
	t.Parallel()

	m := New()
	t.Run("with fn error", func(t *testing.T) {
		err := m.Do(context.TODO(), func(tx db.Txn) error { return tea.NewError("with fn error") })
		assert.NotNil(t, err)
	})

	t.Run("timeout", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.TODO(), 0)
		defer cancel()
		err := m.Do(ctx, func(tx db.Txn) error { return nil })
		assert.NotNil(t, err)
	})

	t.Run("without fn error", func(t *testing.T) {
		err := m.Do(context.TODO(), func(tx db.Txn) error { return nil })
		assert.Nil(t, err)
	})
}

func TestMapper_Txn(t *testing.T) {
	t.Parallel()

	m := New()

	t.Run("bad commit", func(t *testing.T) {
		tx := m.Txn(context.TODO())
		tx.Insert("", "foo", "bar")
		tx.Rollback()
		assert.NotNil(t, tx.Commit())
	})

	t.Run("child", func(t *testing.T) {
		tx := m.Txn(m.Txn(context.TODO()))
		assert.False(t, tx.root)
		assert.Nil(t, tx.Commit())
		assert.Nil(t, tx.Rollback())
	})
}

func TestTxn_Insert(t *testing.T) {
	t.Parallel()

	m := New()

	t.Run("can insert", func(t *testing.T) {
		tx := m.Txn(context.TODO())
		defer tx.Rollback()
		assert.Nil(t, tx.Insert("", "foo", "bar"))
	})
}

func TestTxn_Update(t *testing.T) {
	t.Parallel()

	m := New()
	tx := m.Txn(context.TODO())
	tx.Insert("", "foo", "bar")
	tx.Commit()

	t.Run("can update", func(t *testing.T) {
		tx := m.Txn(context.TODO())
		defer tx.Rollback()
		assert.Nil(t, tx.Update("", "foo", "bar"))
	})
}

func TestTxn_Remove(t *testing.T) {
	t.Parallel()

	m := New()
	tx := m.Txn(context.TODO())
	tx.Insert("", "foo", "bar")
	tx.Commit()

	t.Run("can remove", func(t *testing.T) {
		tx := m.Txn(context.TODO())
		defer tx.Rollback()
		assert.Nil(t, tx.Remove("", "foo"))
	})
}

func TestTxn_Get(t *testing.T) {
	t.Parallel()

	m := New()
	tx := m.Txn(context.TODO())
	tx.Insert("", "foo", "bar")
	tx.Commit()

	t.Run("bad key", func(t *testing.T) {
		tx := m.Txn(context.TODO())
		defer tx.Rollback()
		var v string
		assert.NotNil(t, tx.Get("", func() {}, &v))
	})

	t.Run("not found", func(t *testing.T) {
		tx := m.Txn(context.TODO())
		defer tx.Rollback()
		var v string
		assert.NotNil(t, tx.Get("", "not found", &v))
	})

	t.Run("read batch size exhausted", func(t *testing.T) {
		tx := m.Txn(context.TODO())
		defer tx.Rollback()
		var v string
		tx.Get("", "foo", &v)
		assert.NotNil(t, tx.Get("", "foo", &v))
	})

	t.Run("can get", func(t *testing.T) {
		tx := m.Txn(context.TODO())
		defer tx.Rollback()
		var v string
		assert.Nil(t, tx.Get("", "foo", &v))
		tx.Commit()
		tx.cache.Wait()
	})

	t.Run("can get cached", func(t *testing.T) {
		tx := m.Txn(context.TODO())
		defer tx.Rollback()
		var v string
		assert.Nil(t, tx.Get("", "foo", &v))
	})
}

func TestTxn_List(t *testing.T) {
	t.Parallel()

	m := New()
	tx := m.Txn(context.TODO())
	tx.Insert("", "foo", "bar")
	tx.Commit()

	t.Run("not found", func(t *testing.T) {
		tx := m.Txn(context.TODO())
		defer tx.Rollback()
		var v []string
		assert.NotNil(t, tx.List("", &v, db.Limit(0)))
	})

	t.Run("read batch size exhausted", func(t *testing.T) {
		tx := m.Txn(context.TODO())
		defer tx.Rollback()
		var v []string
		tx.List("", &v)
		assert.NotNil(t, tx.List("", &v))
	})

	t.Run("can list", func(t *testing.T) {
		tx := m.Txn(context.TODO())
		defer tx.Rollback()
		var v []string
		assert.Nil(t, tx.List("", &v))
		tx.Commit()
		tx.cache.Wait()
	})

	t.Run("can list cached", func(t *testing.T) {
		tx := m.Txn(context.TODO())
		defer tx.Rollback()
		var v []string
		assert.Nil(t, tx.List("", &v))
	})
}
