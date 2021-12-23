package sql

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"testing/fstest"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/pghq/go-tea"
	"github.com/stretchr/testify/assert"

	"github.com/pghq/go-ark/database"
)

var postgres *DB

func TestPostgresDB_Ping(t *testing.T) {
	t.Parallel()

	t.Run("ping", func(t *testing.T) {
		assert.Nil(t, postgres.Ping(context.TODO()))
	})
}

func TestPostgresDB_Txn(t *testing.T) {
	t.Parallel()

	t.Run("write", func(t *testing.T) {
		tx := postgres.Txn(context.TODO())
		assert.NotNil(t, tx)
	})

	t.Run("read only", func(t *testing.T) {
		tx := postgres.Txn(context.TODO(), database.ReadOnly())
		assert.NotNil(t, tx)
	})

	t.Run("can rollback", func(t *testing.T) {
		tx := postgres.Txn(context.TODO())
		err := tx.Rollback()
		assert.Nil(t, err)
	})

	t.Run("can commit", func(t *testing.T) {
		tx := postgres.Txn(context.TODO())
		assert.NotNil(t, tx)
		err := tx.Commit()
		assert.Nil(t, err)
	})
}

func TestPostgresTxn_Insert(t *testing.T) {
	t.Parallel()

	t.Run("bad value", func(t *testing.T) {
		tx := postgres.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Insert("tests", database.Id(""), func() {})
		assert.NotNil(t, err)
	})

	t.Run("bad sql", func(t *testing.T) {
		tx := postgres.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Insert("", database.NamedKey(true, "foo"), map[string]interface{}{"id": "foo"})
		assert.NotNil(t, err)
	})

	t.Run("bad exec", func(t *testing.T) {
		tx := postgres.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Insert("tests", database.NamedKey(true, "foo"), map[string]interface{}{"id": "foo", "fn": func() {}})
		assert.NotNil(t, err)
	})

	t.Run("ok", func(t *testing.T) {
		tx := postgres.Txn(context.TODO())
		defer tx.Commit()
		err := tx.Insert("tests", database.NamedKey(true, "foo"), map[string]interface{}{"id": "foo"})
		assert.Nil(t, err)
	})

	t.Run("integrity violation", func(t *testing.T) {
		tx := postgres.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Insert("tests", database.NamedKey(true, "foo"), map[string]interface{}{"id": "foo"})
		assert.NotNil(t, err)
	})
}

func TestPostgresTxn_Update(t *testing.T) {
	t.Parallel()

	t.Run("bad value", func(t *testing.T) {
		tx := postgres.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Update("tests", database.NamedKey(true, "foo"), func() {})
		assert.NotNil(t, err)
	})

	t.Run("bad sql", func(t *testing.T) {
		tx := postgres.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Update("", database.NamedKey(true, "foo"), map[string]interface{}{"id": "foo"})
		assert.NotNil(t, err)
	})

	t.Run("bad exec", func(t *testing.T) {
		tx := postgres.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Update("tests", database.NamedKey(true, "foo"), map[string]interface{}{"id": "foo", "fn": func() {}})
		assert.NotNil(t, err)
	})

	t.Run("ok", func(t *testing.T) {
		tx := postgres.Txn(context.TODO())
		defer tx.Rollback()
		assert.Nil(t, tx.Insert("tests", database.NamedKey(true, "ok"), map[string]interface{}{"id": "foo"}))
		err := tx.Update("tests", database.NamedKey(true, "ok"), map[string]interface{}{"id": "foo"})
		assert.Nil(t, err)
	})
}

func TestPostgresTxn_Get(t *testing.T) {
	t.Parallel()

	t.Run("missing key name", func(t *testing.T) {
		tx := postgres.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Get("tests", database.NamedKey(true, "foo"), nil)
		assert.NotNil(t, err)
	})

	t.Run("bad sql", func(t *testing.T) {
		tx := postgres.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Get("", database.NamedKey(true, "foo"), nil)
		assert.NotNil(t, err)
	})

	t.Run("bad exec", func(t *testing.T) {
		tx := postgres.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Get("tests", database.NamedKey(true, "foo"), nil, database.Fields("id"))
		assert.NotNil(t, err)
	})

	t.Run("not found", func(t *testing.T) {
		tx := postgres.Txn(context.TODO())
		defer tx.Rollback()
		var id string
		err := tx.Get("tests", database.NamedKey(true, "not found"), &id, database.Fields("id"))
		assert.NotNil(t, err)
		assert.False(t, tea.IsFatal(err))
	})

	t.Run("ok for single field", func(t *testing.T) {
		tx := postgres.Txn(context.TODO())
		defer tx.Rollback()
		tx.Insert("tests", database.NamedKey(true, "foo1"), map[string]interface{}{"id": "foo1"})
		var id string
		err := tx.Get("tests", database.NamedKey(true, "foo1"), &id, database.Fields("id"))
		assert.Nil(t, err)
		assert.Equal(t, "foo1", id)
	})

	t.Run("ok for multiple fields", func(t *testing.T) {
		tx := postgres.Txn(context.TODO())
		defer tx.Rollback()
		tx.Insert("tests", database.NamedKey(true, "foo2"), map[string]interface{}{"id": "foo2", "name": "bar2"})
		type data struct {
			Id   *string `db:"id"`
			Name *string `db:"name"`
		}
		var doc data
		err := tx.Get("tests", database.NamedKey(true, "foo2"), &doc, database.Fields("id", "name"))
		assert.Nil(t, err)
		assert.Equal(t, "foo2", *doc.Id)
		assert.Equal(t, "bar2", *doc.Name)
	})
}

func TestPostgresTxn_Remove(t *testing.T) {
	t.Parallel()

	t.Run("bad sql", func(t *testing.T) {
		tx := postgres.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Remove("", database.NamedKey(true, "foo"))
		assert.NotNil(t, err)
	})

	t.Run("ok", func(t *testing.T) {
		tx := postgres.Txn(context.TODO())
		defer tx.Rollback()
		tx.Insert("tests", database.NamedKey(true, "foo"), map[string]interface{}{"id": "foo"})
		err := tx.Remove("tests", database.NamedKey(true, "foo"))
		assert.Nil(t, err)
	})
}

func TestPostgresTxn_List(t *testing.T) {
	t.Parallel()

	t.Run("bad sql", func(t *testing.T) {
		tx := postgres.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.List("", nil)
		assert.NotNil(t, err)
	})

	t.Run("bad exec", func(t *testing.T) {
		tx := postgres.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.List("tests", nil, database.Fields("id"))
		assert.NotNil(t, err)
	})

	t.Run("ok for single field", func(t *testing.T) {
		tx := postgres.Txn(context.TODO())
		defer tx.Rollback()
		tx.Insert("tests", database.NamedKey(true, "foo1"), map[string]interface{}{"id": "foo1", "name": "bar1"})
		tx.Insert("tests", database.NamedKey(true, "foo2"), map[string]interface{}{"id": "foo2", "name": "bar2"})
		var ids []string
		err := tx.List("tests", &ids, database.Eq("tests.name", "bar2"), database.Fields("id"))
		assert.Nil(t, err)
		assert.Equal(t, []string{"foo2"}, ids)
	})

	t.Run("uses opts", func(t *testing.T) {
		tx := postgres.Txn(context.TODO())
		defer tx.Rollback()
		tx.Insert("tests", database.NamedKey(true, "foo3"), map[string]interface{}{"id": "foo3", "name": "bar3"})
		tx.Insert("tests", database.NamedKey(true, "foo4"), map[string]interface{}{"id": "foo4", "name": "bar4", "num": 1})
		type data struct {
			Id   *string `db:"id"`
			Name *string `db:"name"`
		}
		var d []data
		opts := []database.QueryOption{
			database.Eq("name", "bar4"),
			database.NotEq("name", "bar4"),
			database.Fields("tests.id", "tests.name"),
			database.XEq("name", "%bar%"),
			database.Limit(1),
			database.OrderBy("name"),
			database.Gt("num", 0),
			database.Lt("num", 2),
			database.Table("LEFT JOIN units ON units.id = tests.id"),
			database.Filter("name = 'bar4'"),
		}
		err := tx.List("tests", &d, opts...)
		assert.Nil(t, err)
	})
}

// NewTestPostgresDB creates a new application with preloaded testdata
func NewTestPostgresDB() (*DB, func()) {
	pool, err := dockertest.NewPool("")
	must(err)

	opts := dockertest.RunOptions{
		Repository: "postgres",
		Tag:        "12",
		Env: []string{
			"POSTGRES_USER=postgres",
			"POSTGRES_PASSWORD=secret",
			"POSTGRES_DB=db",
			"listen_addresses='*'",
		},
	}

	resource, err := pool.RunWithOptions(&opts, func(config *docker.HostConfig) {
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{Name: "no"}
	})
	must(err)
	must(resource.Expire(60))
	pool.MaxWait = 60 * time.Second
	dsn := fmt.Sprintf("postgres://postgres:secret@%s/db?sslmode=disable", resource.GetHostPort("5432/tcp"))
	conn, _ := sql.Open("postgres", dsn)
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	must(err)
	must(pool.Retry(conn.Ping))
	databaseURL, _ := url.Parse(dsn)

	fs := fstest.MapFS{
		"migrations/00001_test.sql": &fstest.MapFile{
			Data: []byte("-- +goose Up\nCREATE TABLE IF NOT EXISTS tests (id text unique, name text, num int);\nCREATE TABLE IF NOT EXISTS units (id text);"),
		},
	}

	db := NewDB("postgres", databaseURL, database.Migrate(fs, "migrations", "migrations"))
	return db, func() {
		s.Close()
		_ = pool.Purge(resource)
	}
}
