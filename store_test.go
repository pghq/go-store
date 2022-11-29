package store

import (
	"context"
	"os"
	"testing"
	"testing/fstest"
	"time"

	"github.com/pghq/go-tea/trail"
	"github.com/stretchr/testify/assert"

	"github.com/pghq/go-store/db"
	"github.com/pghq/go-store/db/pg/pgtest"
)

var (
	dsn   string
	store *Store
)

func TestMain(m *testing.M) {
	trail.Testing()
	var cleanup func() error
	var err error
	dsn, cleanup, err = pgtest.Start()
	if err != nil {
		panic(err)
	}

	store, err = New(WithDSN(dsn), WithMigration(fstest.MapFS{
		"migrations/00001_test.sql": &fstest.MapFile{
			Data: []byte("-- +goose Up\nCREATE TABLE tests (id text primary key, name text, num int); \n create index idx_tests_name ON tests (name);"),
		},
	}))
	if err != nil {
		panic(err)
	}

	code := m.Run()
	if err := cleanup(); err != nil {
		panic(err)
	}

	os.Exit(code)
}

func TestNew(t *testing.T) {
	trail.Testing()
	t.Parallel()

	t.Run("bad dsn", func(t *testing.T) {
		// https://stackoverflow.com/questions/48671938/go-url-parsestring-fails-with-certain-user-names-or-passwords
		_, err := New(WithDSN("sql://user:abc{DEf1=ghi@example.com:5432/db?sslmode=require"))
		assert.NotNil(t, err)
	})

	t.Run("ok", func(t *testing.T) {
		store, _ := New(WithDSN(dsn), WithMigration(nil), WithPg())
		assert.NotNil(t, store)
	})
}

func TestStore_Do(t *testing.T) {
	trail.Testing()
	t.Parallel()

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

	t.Run("ok", func(t *testing.T) {
		assert.Nil(t, store.Do(context.TODO(), func(tx Txn) error {
			return tx.Add("tests", map[string]interface{}{"id": "1234"})
		}))
	})
}

func TestTxn_Edit(t *testing.T) {
	trail.Testing()
	t.Parallel()

	_ = store.Do(context.TODO(), func(tx Txn) error {
		return tx.Add("tests", map[string]interface{}{"id": "edit:1234"})
	})

	t.Run("ok", func(t *testing.T) {
		assert.Nil(t, store.Do(context.TODO(), func(tx Txn) error {
			return tx.Edit("tests", db.Sql("id = 'edit:1234'"), map[string]interface{}{"id": "edit:1234"})
		}))
	})
}

func TestTxn_Remove(t *testing.T) {
	trail.Testing()
	t.Parallel()

	_ = store.Do(context.TODO(), func(tx Txn) error {
		return tx.Add("tests", map[string]interface{}{"id": "remove:1234"})
	})

	t.Run("ok", func(t *testing.T) {
		assert.Nil(t, store.Do(context.TODO(), func(tx Txn) error {
			return tx.Remove("tests", db.Sql("id = 'remove:1234'"))
		}))
	})
}

func TestTxn_One(t *testing.T) {
	trail.Testing()
	t.Parallel()

	_ = store.Do(context.TODO(), func(tx Txn) error {
		return tx.Add("tests", map[string]interface{}{"id": "one:1234"})
	})

	t.Run("bad query", func(t *testing.T) {
		var v map[string]interface{}
		assert.NotNil(t, store.Do(context.TODO(), func(tx Txn) error {
			return tx.One(db.Sql("= '1234'"), &v)
		}))
	})

	t.Run("ok", func(t *testing.T) {
		var v struct{ Id string }
		assert.Nil(t, store.Do(context.TODO(), func(tx Txn) error {
			return tx.One(db.Sql("SELECT id FROM tests WHERE id = 'one:1234'"), &v)
		}))
		assert.Equal(t, "one:1234", v.Id)
	})

	t.Run("cached", func(t *testing.T) {
		t.Run("bad cache value", func(t *testing.T) {
			var v struct{ Id string }
			assert.NotNil(t, store.Do(context.TODO(), func(tx Txn) error {
				_ = tx.One(db.Sql("SELECT id FROM tests WHERE id = 'one:1234'"), &v, QueryTTL(time.Minute))
				tx.store.cache.Wait()
				return tx.One(db.Sql("SELECT id FROM tests WHERE id = 'one:1234'"), func() {}, QueryTTL(time.Minute))
			}))
		})

		t.Run("ok", func(t *testing.T) {
			var v struct{ Id string }
			assert.Nil(t, store.Do(context.TODO(), func(tx Txn) error {
				_ = tx.One(db.Sql("SELECT id FROM tests WHERE id = 'one:1234'"), &v, QueryTTL(time.Minute))
				tx.store.cache.Wait()
				return tx.One(db.Sql("SELECT id FROM tests WHERE id = 'one:1234'"), &v, QueryTTL(time.Minute))
			}))
			assert.Equal(t, "one:1234", v.Id)
		})
	})
}

func TestTxn_All(t *testing.T) {
	trail.Testing()
	t.Parallel()

	_ = store.Do(context.TODO(), func(tx Txn) error {
		return tx.Add("tests", map[string]interface{}{"id": "all:1234"})
	})

	t.Run("bad query", func(t *testing.T) {
		var v []map[string]interface{}
		assert.NotNil(t, store.Do(context.TODO(), func(tx Txn) error {
			return tx.All(db.Sql("= '1234'"), &v)
		}))
	})

	t.Run("ok", func(t *testing.T) {
		var v []struct{ Id string }
		assert.Nil(t, store.Do(context.TODO(), func(tx Txn) error {
			return tx.All(db.Sql("SELECT id FROM tests WHERE id = 'all:1234'"), &v)
		}))
		assert.Equal(t, "all:1234", v[0].Id)
	})

	t.Run("cached", func(t *testing.T) {
		t.Run("bad cache value", func(t *testing.T) {
			var v []struct{ Id string }
			assert.NotNil(t, store.Do(context.TODO(), func(tx Txn) error {
				_ = tx.All(db.Sql("SELECT id FROM tests WHERE id = 'all:1234'"), &v, QueryTTL(time.Minute))
				tx.store.cache.Wait()
				return tx.All(db.Sql("SELECT id FROM tests WHERE id = 'all:1234'"), func() {}, QueryTTL(time.Minute))
			}))
		})

		t.Run("ok", func(t *testing.T) {
			var v []struct{ Id string }
			assert.Nil(t, store.Do(context.TODO(), func(tx Txn) error {
				_ = tx.All(db.Sql("SELECT id FROM tests WHERE id = 'all:1234'"), &v, QueryTTL(time.Minute))
				return tx.All(db.Sql("SELECT id FROM tests WHERE id = 'all:1234'"), &v, QueryTTL(time.Minute))
			}))
			assert.Equal(t, "all:1234", v[0].Id)
		})
	})
}

func TestTxn_Batch(t *testing.T) {
	trail.Testing()
	t.Parallel()

	_ = store.Do(context.TODO(), func(tx Txn) error {
		return tx.Add("tests", map[string]interface{}{"id": "batch.query:1234"})
	})

	t.Run("bad query", func(t *testing.T) {
		var v []map[string]interface{}
		assert.NotNil(t, store.Do(context.TODO(), func(tx Txn) error {
			batch := db.Batch{}
			batch.One(db.Sql("= '1234'"), &v)
			return tx.Batch(batch)
		}))
	})

	t.Run("ok", func(t *testing.T) {
		var v []struct{ Id string }
		assert.Nil(t, store.Do(context.TODO(), func(tx Txn) error {
			batch := db.Batch{}
			batch.All(db.Sql("SELECT id FROM tests WHERE id = 'batch.query:1234'"), &v)
			return tx.Batch(batch)
		}))
		assert.Equal(t, "batch.query:1234", v[0].Id)
	})

	t.Run("cached", func(t *testing.T) {
		t.Run("bad cache value", func(t *testing.T) {
			var v []struct{ Id string }
			assert.NotNil(t, store.Do(context.TODO(), func(tx Txn) error {
				batch := db.Batch{}
				batch.All(db.Sql("SELECT id FROM tests WHERE id = 'batch.query:1234'"), &v)
				_ = tx.Batch(batch, QueryTTL(time.Minute))
				tx.store.cache.Wait()

				batch = db.Batch{}
				batch.All(db.Sql("SELECT id FROM tests WHERE id = 'batch.query:1234'"), func() {})
				return tx.Batch(batch, QueryTTL(time.Minute))
			}))
		})

		t.Run("ok", func(t *testing.T) {
			var v []struct{ Id string }
			assert.Nil(t, store.Do(context.TODO(), func(tx Txn) error {
				batch := db.Batch{}
				batch.All(db.Sql("SELECT id FROM tests WHERE id = 'batch.query:1234'"), &v)
				_ = tx.Batch(batch, QueryTTL(time.Minute))
				tx.store.cache.Wait()
				return tx.Batch(batch, QueryTTL(time.Minute))
			}))
			assert.Equal(t, "batch.query:1234", v[0].Id)
		})
	})
}
