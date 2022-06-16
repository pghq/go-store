package store

import (
	"context"
	_ "github.com/mattn/go-sqlite3"
	"github.com/pghq/go-tea/trail"
	"github.com/stretchr/testify/assert"
	"testing"
	"testing/fstest"
)

func TestNew(t *testing.T) {
	trail.Testing()
	t.Parallel()

	t.Run("bad dsn", func(t *testing.T) {
		// https://stackoverflow.com/questions/48671938/go-url-parsestring-fails-with-certain-user-names-or-passwords
		_, err := New(WithDSN("sqlite3", "sql://user:abc{DEf1=ghi@example.com:5432/db?sslmode=require"))
		assert.NotNil(t, err)
	})

	t.Run("ok", func(t *testing.T) {
		store, _ := New(WithDSN("sqlite3", ":memory:"), WithMigration(nil))
		assert.NotNil(t, store)
	})
}

func TestStore_Do(t *testing.T) {
	trail.Testing()
	t.Parallel()

	store, _ := New(WithDSN("sqlite3", ":memory:"), WithMigration(nil))
	t.Run("bad context", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.TODO(), 1)
		defer cancel()
		assert.NotNil(t, store.Do(ctx, nil))
	})

	t.Run("bad callback response", func(t *testing.T) {
		assert.NotNil(t, store.Do(context.TODO(), func(tx Txn) error {
			return trail.NewError("")
		}))
	})

	t.Run("ok", func(t *testing.T) {
		assert.Nil(t, store.Do(context.TODO(), func(tx Txn) error {
			return store.Do(tx.Context(), func(tx Txn) error {
				return nil
			})
		}))
	})
}

func TestTxn_Add(t *testing.T) {
	trail.Testing()
	t.Parallel()

	store, _ := New(WithDSN("sqlite3", "file:add?mode=memory&cache=shared"), WithMigration(fstest.MapFS{
		"migrations/00001_test.sql": &fstest.MapFile{
			Data: []byte("-- +goose Up\nCREATE TABLE tests (id text primary key, name text, num int); create index idx_tests_name ON tests (name);"),
		},
	}))

	t.Run("ok", func(t *testing.T) {
		assert.Nil(t, store.Do(context.TODO(), func(tx Txn) error {
			return tx.Add("tests", map[string]interface{}{"id": "1234"})
		}))
	})
}

func TestTxn_Edit(t *testing.T) {
	trail.Testing()
	t.Parallel()

	store, _ := New(WithDSN("sqlite3", "file:edit?mode=memory&cache=shared"), WithMigration(fstest.MapFS{
		"migrations/00001_test.sql": &fstest.MapFile{
			Data: []byte("-- +goose Up\nCREATE TABLE tests (id text primary key, name text, num int); create index idx_tests_name ON tests (name);"),
		},
	}))

	_ = store.Do(context.TODO(), func(tx Txn) error {
		return tx.Add("tests", map[string]interface{}{"id": "1234"})
	})

	t.Run("ok", func(t *testing.T) {
		assert.Nil(t, store.Do(context.TODO(), func(tx Txn) error {
			return tx.Edit(spec("id = '1234'"), map[string]interface{}{"id": "1234"})
		}))
	})
}

func TestTxn_Remove(t *testing.T) {
	trail.Testing()
	t.Parallel()

	store, _ := New(WithDSN("sqlite3", "file:remove?mode=memory&cache=shared"), WithMigration(fstest.MapFS{
		"migrations/00001_test.sql": &fstest.MapFile{
			Data: []byte("-- +goose Up\nCREATE TABLE tests (id text primary key, name text, num int); create index idx_tests_name ON tests (name);"),
		},
	}))

	_ = store.Do(context.TODO(), func(tx Txn) error {
		return tx.Add("tests", map[string]interface{}{"id": "1234"})
	})

	t.Run("ok", func(t *testing.T) {
		assert.Nil(t, store.Do(context.TODO(), func(tx Txn) error {
			return tx.Remove(spec("id = '1234'"))
		}))
	})
}

func TestTxn_First(t *testing.T) {
	trail.Testing()
	t.Parallel()

	store, _ := New(WithDSN("sqlite3", "file:first?mode=memory&cache=shared"), WithMigration(fstest.MapFS{
		"migrations/00001_test.sql": &fstest.MapFile{
			Data: []byte("-- +goose Up\nCREATE TABLE tests (id text primary key, name text, num int); create index idx_tests_name ON tests (name);"),
		},
	}))

	_ = store.Do(context.TODO(), func(tx Txn) error {
		return tx.Add("tests", map[string]interface{}{"id": "1234"})
	})

	t.Run("bad query", func(t *testing.T) {
		var v map[string]interface{}
		assert.NotNil(t, store.Do(context.TODO(), func(tx Txn) error {
			return tx.First(spec("= '1234'"), &v)
		}))
	})

	t.Run("ok", func(t *testing.T) {
		var v struct{ Id string }
		assert.Nil(t, store.Do(context.TODO(), func(tx Txn) error {
			return tx.First(spec("SELECT id FROM tests WHERE id = '1234'"), &v)
		}))
		assert.Equal(t, "1234", v.Id)
	})

	t.Run("cached", func(t *testing.T) {
		t.Run("bad cache value", func(t *testing.T) {
			var v struct{ Id string }
			assert.NotNil(t, store.Do(context.TODO(), func(tx Txn) error {
				_ = tx.First(spec("SELECT id FROM tests WHERE id = '1234'"), &v)
				return tx.First(spec("SELECT id FROM tests WHERE id = '1234'"), func() {})
			}))
		})

		t.Run("ok", func(t *testing.T) {
			var v struct{ Id string }
			assert.Nil(t, store.Do(context.TODO(), func(tx Txn) error {
				_ = tx.First(spec("SELECT id FROM tests WHERE id = '1234'"), &v)
				return tx.First(spec("SELECT id FROM tests WHERE id = '1234'"), &v)
			}))
			assert.Equal(t, "1234", v.Id)
		})
	})
}

func TestTxn_List(t *testing.T) {
	trail.Testing()
	t.Parallel()

	store, _ := New(WithDSN("sqlite3", "file:list?mode=memory&cache=shared"), WithMigration(fstest.MapFS{
		"migrations/00001_test.sql": &fstest.MapFile{
			Data: []byte("-- +goose Up\nCREATE TABLE tests (id text primary key, name text, num int); create index idx_tests_name ON tests (name);"),
		},
	}))

	_ = store.Do(context.TODO(), func(tx Txn) error {
		return tx.Add("tests", map[string]interface{}{"id": "1234"})
	})

	t.Run("bad query", func(t *testing.T) {
		var v []map[string]interface{}
		assert.NotNil(t, store.Do(context.TODO(), func(tx Txn) error {
			return tx.List(spec("= '1234'"), &v)
		}))
	})

	t.Run("ok", func(t *testing.T) {
		var v []struct{ Id string }
		assert.Nil(t, store.Do(context.TODO(), func(tx Txn) error {
			return tx.List(spec("SELECT id FROM tests WHERE id = '1234'"), &v)
		}))
		assert.Equal(t, "1234", v[0].Id)
	})

	t.Run("cached", func(t *testing.T) {
		t.Run("bad cache value", func(t *testing.T) {
			var v []struct{ Id string }
			assert.NotNil(t, store.Do(context.TODO(), func(tx Txn) error {
				_ = tx.List(spec("SELECT id FROM tests WHERE id = '1234'"), &v)
				return tx.List(spec("SELECT id FROM tests WHERE id = '1234'"), func() {})
			}))
		})

		t.Run("ok", func(t *testing.T) {
			var v []struct{ Id string }
			assert.Nil(t, store.Do(context.TODO(), func(tx Txn) error {
				_ = tx.List(spec("SELECT id FROM tests WHERE id = '1234'"), &v)
				return tx.List(spec("SELECT id FROM tests WHERE id = '1234'"), &v)
			}))
			assert.Equal(t, "1234", v[0].Id)
		})
	})
}

type spec string

func (s spec) Id() interface{} {
	return string(s)
}

func (s spec) Collection() string {
	return "tests"
}

func (s spec) ToSql() (string, []interface{}, error) {
	return string(s), nil, nil
}
