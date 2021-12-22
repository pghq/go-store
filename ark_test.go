package ark

import (
	"context"
	"os"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pghq/go-tea"
	"github.com/stretchr/testify/assert"

	"github.com/pghq/go-ark/database"
)

func TestMain(m *testing.M) {
	tea.Testing()
	os.Exit(m.Run())
}

func TestNew(t *testing.T) {
	t.Parallel()

	t.Run("bad dsn", func(t *testing.T) {
		// https://stackoverflow.com/questions/48671938/go-url-parsestring-fails-with-certain-user-names-or-passwords
		assert.NotNil(t, New("postgres://user:abc{DEf1=ghi@example.com:5432/db?sslmode=require"))
	})

	t.Run("unrecognized technology", func(t *testing.T) {
		m := New("mongodb://")
		assert.NotNil(t, m.Txn(context.TODO()).Commit())
		assert.NotNil(t, m.Txn(context.TODO()).Rollback())
		assert.NotNil(t, m.Txn(context.TODO()).Get("", "", nil))
		assert.NotNil(t, m.Txn(context.TODO()).Insert("", "", nil))
		assert.NotNil(t, m.Txn(context.TODO()).InsertTTL("", "", nil, 0))
		assert.NotNil(t, m.Txn(context.TODO()).Remove("", "", nil))
		assert.NotNil(t, m.Txn(context.TODO()).Update("", "", nil))
		assert.NotNil(t, m.Txn(context.TODO()).List("", ""))
		assert.NotNil(t, m.Do(context.TODO(), func(tx Txn) error { return nil }))
		assert.NotNil(t, m.View(context.TODO(), func(tx Txn) error { return nil }))
	})

	t.Run("with error", func(t *testing.T) {
		m := New("memory://")
		m.SetError(tea.Err("error"))
		assert.NotNil(t, m.Error())
	})

	t.Run("with default db", func(t *testing.T) {
		m := New("memory://")
		assert.NotNil(t, m)
		assert.Nil(t, m.err)
	})

	t.Run("redis", func(t *testing.T) {
		m := New("redis://user:pass@example.com/db")
		assert.NotNil(t, m)
		assert.NotNil(t, m.err)
	})

	t.Run("postgres", func(t *testing.T) {
		m := New("postgres://user:pass@example.com/db")
		assert.NotNil(t, m)
		assert.NotNil(t, m.err)
	})
}

func TestMapper_View(t *testing.T) {
	t.Parallel()

	m := New("memory://")
	t.Run("with fn error", func(t *testing.T) {
		err := m.View(context.TODO(), func(tx Txn) error { return tea.Err("with fn error") })
		assert.NotNil(t, err)
	})

	t.Run("timeout", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.TODO(), 0)
		defer cancel()
		err := m.View(ctx, func(tx Txn) error { return nil })
		assert.NotNil(t, err)
	})

	t.Run("without fn error", func(t *testing.T) {
		err := m.View(context.TODO(), func(tx Txn) error { return nil })
		assert.Nil(t, err)
	})
}

func TestMapper_Do(t *testing.T) {
	t.Parallel()

	m := New("memory://")
	t.Run("with fn error", func(t *testing.T) {
		err := m.Do(context.TODO(), func(tx Txn) error { return tea.Err("with fn error") })
		assert.NotNil(t, err)
	})

	t.Run("timeout", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.TODO(), 0)
		defer cancel()
		err := m.Do(ctx, func(tx Txn) error { return nil })
		assert.NotNil(t, err)
	})

	t.Run("without fn error", func(t *testing.T) {
		err := m.Do(context.TODO(), func(tx Txn) error { return nil })
		assert.Nil(t, err)
	})
}

func TestMapper_Txn(t *testing.T) {
	t.Parallel()

	m := New("memory://")

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

	m := New("memory://")

	t.Run("ok", func(t *testing.T) {
		tx := m.Txn(context.TODO())
		defer tx.Rollback()
		assert.Nil(t, tx.Insert("", "foo", "bar"))
	})
}

func TestTxn_InsertTTL(t *testing.T) {
	t.Parallel()

	m := New("memory://")

	t.Run("ok", func(t *testing.T) {
		tx := m.Txn(context.TODO())
		defer tx.Rollback()
		assert.Nil(t, tx.InsertTTL("", "foo", "bar", 0))
	})
}

func TestTxn_Update(t *testing.T) {
	t.Parallel()

	m := New("memory://")
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

	m := New("memory://")
	tx := m.Txn(context.TODO())
	tx.Insert("", "foo", "bar")
	tx.Commit()

	t.Run("can remove", func(t *testing.T) {
		tx := m.Txn(context.TODO())
		defer tx.Rollback()
		assert.Nil(t, tx.Remove("", "foo", nil))
	})
}

func TestTxn_Get(t *testing.T) {
	t.Parallel()

	m := New("memory://")
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

	m := New("memory://")
	tx := m.Txn(context.TODO())
	tx.Insert("", "foo", "bar")
	tx.Commit()

	t.Run("not found", func(t *testing.T) {
		tx := m.Txn(context.TODO())
		defer tx.Rollback()
		var v []string
		assert.NotNil(t, tx.List("", &v, database.Limit(0)))
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
