package driver

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
	"github.com/pghq/go-tea/trail"
	"github.com/stretchr/testify/assert"

	"github.com/pghq/go-ark/database"
)

var postgres *SQL

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

	t.Run("context timeout", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 0)
		defer cancel()
		db := postgres
		assert.NotNil(t, db.Txn(ctx).List("", database.Query{}, ""))
		assert.NotNil(t, db.Txn(ctx).Commit())
		assert.NotNil(t, db.Txn(ctx).Rollback())
		assert.NotNil(t, db.Txn(ctx).Get("", database.Query{}, nil))
		assert.NotNil(t, db.Txn(ctx).Insert("", nil))
		assert.NotNil(t, db.Txn(ctx).Remove("", database.Query{}))
		assert.NotNil(t, db.Txn(ctx).Update("", database.Query{}, nil))
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

	t.Run("context timeout", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.TODO(), 10*time.Millisecond)
		defer cancel()
		tx := postgres.Txn(ctx)
		assert.NotNil(t, tx)
		<-time.After(10 * time.Millisecond)
		err := tx.Commit()
		assert.NotNil(t, err)
		assert.False(t, trail.IsFatal(err))
	})
}

func TestPostgresTxn_Insert(t *testing.T) {
	t.Parallel()

	t.Run("bad value", func(t *testing.T) {
		tx := postgres.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Insert("tests", func() {})
		assert.NotNil(t, err)
	})

	t.Run("bad sql", func(t *testing.T) {
		tx := postgres.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Insert("", map[string]interface{}{"id": "foo"})
		assert.NotNil(t, err)
	})

	t.Run("bad exec", func(t *testing.T) {
		tx := postgres.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Insert("tests", map[string]interface{}{"id": "foo", "fn": func() {}})
		assert.NotNil(t, err)
	})

	t.Run("ok", func(t *testing.T) {
		tx := postgres.Txn(context.TODO())
		defer tx.Commit()
		err := tx.Insert("tests", map[string]interface{}{"id": "insert:foo"})
		assert.Nil(t, err)
	})

	t.Run("integrity violation", func(t *testing.T) {
		tx := postgres.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Insert("tests", map[string]interface{}{"id": "insert:foo"})
		assert.NotNil(t, err)
	})

	t.Run("suffix", func(t *testing.T) {
		tx := postgres.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Insert("tests", map[string]interface{}{"id": "insert:foo"})
		assert.NotNil(t, err)
	})
}

func TestPostgresTxn_Update(t *testing.T) {
	t.Parallel()

	t.Run("bad value", func(t *testing.T) {
		tx := postgres.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Update("tests", database.Query{}, func() {})
		assert.NotNil(t, err)
	})

	t.Run("bad sql", func(t *testing.T) {
		tx := postgres.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Update("", database.Query{}, map[string]interface{}{"id": "foo"})
		assert.NotNil(t, err)
	})

	t.Run("bad exec", func(t *testing.T) {
		tx := postgres.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Update("tests", database.Query{}, map[string]interface{}{"id": "foo", "fn": func() {}})
		assert.NotNil(t, err)
	})

	t.Run("ok", func(t *testing.T) {
		tx := postgres.Txn(context.TODO())
		defer tx.Rollback()
		assert.Nil(t, tx.Insert("tests", map[string]interface{}{"id": "ok"}))
		err := tx.Update("tests", database.Query{Eq: map[string]interface{}{"id": "ok"}}, map[string]interface{}{"id": "ok"})
		assert.Nil(t, err)
	})

	t.Run("use opts", func(t *testing.T) {
		tx := postgres.Txn(context.TODO())
		defer tx.Rollback()

		assert.Nil(t, tx.Insert("tests", map[string]interface{}{"id": "opts:ok"}))
		query := database.Query{
			Fields:  []string{"tests.id", "tests.name"},
			NotEq:   map[string]interface{}{"name": "bar4"},
			XEq:     map[string]interface{}{"name": "%bar%"},
			Limit:   1,
			OrderBy: []string{"name"},
			Eq:      map[string]interface{}{"name": "bar4"},
			Gt:      map[string]interface{}{"num": 0},
			Lt:      map[string]interface{}{"num": 2},
			Px:      map[string]string{"num": "bar"},
			Tables:  []database.Expression{database.Expr("LEFT JOIN units ON units.id = tests.id")},
			Filters: []database.Expression{database.Expr("name = 'bar4'")},
		}

		var doc map[string]interface{}
		err := tx.Update("tests", query, &doc)
		assert.NotNil(t, err)
	})
}

func TestPostgresTxn_Get(t *testing.T) {
	t.Parallel()

	t.Run("bad sql", func(t *testing.T) {
		tx := postgres.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Get("", database.Query{}, nil)
		assert.NotNil(t, err)
	})

	t.Run("bad exec", func(t *testing.T) {
		tx := postgres.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Get("tests", database.Query{Fields: []string{"id"}}, nil)
		assert.NotNil(t, err)
	})

	t.Run("not found", func(t *testing.T) {
		tx := postgres.Txn(context.TODO())
		defer tx.Rollback()
		var id string
		err := tx.Get("tests", database.Query{Eq: map[string]interface{}{"id": "not found"}, Fields: []string{"id"}}, &id)
		assert.NotNil(t, err)
		assert.False(t, trail.IsFatal(err))
	})

	t.Run("ok for single field", func(t *testing.T) {
		tx := postgres.Txn(context.TODO())
		defer tx.Rollback()
		tx.Insert("tests", map[string]interface{}{"id": "get:foo1"})
		var id string
		err := tx.Get("tests", database.Query{Eq: map[string]interface{}{"id": "get:foo1"}, Fields: []string{"id"}}, &id)
		assert.Nil(t, err)
		assert.Equal(t, "get:foo1", id)
	})

	t.Run("ok for multiple fields", func(t *testing.T) {
		tx := postgres.Txn(context.TODO())
		defer tx.Rollback()
		tx.Insert("tests", map[string]interface{}{"id": "get:foo2", "name": "bar2"})
		type data struct {
			Id   *string `db:"id"`
			Name *string `db:"name"`
		}
		var doc data
		err := tx.Get("tests", database.Query{Eq: map[string]interface{}{"id": "get:foo2"}, Fields: []string{"id", "name"}}, &doc)
		assert.Nil(t, err)
		assert.Equal(t, "get:foo2", *doc.Id)
		assert.Equal(t, "bar2", *doc.Name)
	})

	t.Run("use opts", func(t *testing.T) {
		tx := postgres.Txn(context.TODO())
		defer tx.Rollback()
		query := database.Query{
			Fields:  []string{"tests.id", "tests.name"},
			NotEq:   map[string]interface{}{"name": "bar4"},
			XEq:     map[string]interface{}{"name": "%bar%"},
			Limit:   1,
			OrderBy: []string{"name"},
			Eq:      map[string]interface{}{"name": "bar4"},
			Gt:      map[string]interface{}{"num": 0},
			Lt:      map[string]interface{}{"num": 2},
			Px:      map[string]string{"num": "bar"},
			Tables:  []database.Expression{database.Expr("LEFT JOIN units ON units.id = tests.id")},
			Filters: []database.Expression{database.Expr("name = 'bar4'")},
		}
		var doc map[string]interface{}
		err := tx.Get("tests", query, &doc)
		assert.NotNil(t, err)
	})

	t.Run("alias", func(t *testing.T) {
		tx := postgres.Txn(context.TODO())
		defer tx.Rollback()

		tx.Insert("tests", map[string]interface{}{"id": "alias:foo3", "name": "bar3"})
		tx.Insert("tests", map[string]interface{}{"id": "alias:foo4", "name": "bar4", "num": 1})

		var dst map[string]interface{}
		err := tx.Get("tests",
			database.Query{
				Fields: []string{"count"},
				Alias:  map[string]string{"count": "SELECT count(id) FROM units WHERE tests.id = units.id"},
			},
			&dst,
		)
		assert.Nil(t, err)
	})
}

func TestPostgresTxn_Remove(t *testing.T) {
	t.Parallel()

	t.Run("bad sql", func(t *testing.T) {
		tx := postgres.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Remove("", database.Query{Eq: map[string]interface{}{"id": "foo"}})
		assert.NotNil(t, err)
	})

	t.Run("ok", func(t *testing.T) {
		tx := postgres.Txn(context.TODO())
		defer tx.Rollback()
		tx.Insert("tests", map[string]interface{}{"id": "remove:foo"})
		err := tx.Remove("tests", database.Query{Eq: map[string]interface{}{"id": "remove:foo"}})
		assert.Nil(t, err)
	})

	t.Run("use opts", func(t *testing.T) {
		tx := postgres.Txn(context.TODO())
		defer tx.Rollback()
		query := database.Query{
			Fields:  []string{"tests.id", "tests.name"},
			NotEq:   map[string]interface{}{"name": "bar4"},
			XEq:     map[string]interface{}{"name": "%bar%"},
			Limit:   1,
			OrderBy: []string{"name"},
			Eq:      map[string]interface{}{"id": "remove:foo"},
			Gt:      map[string]interface{}{"num": 0},
			Lt:      map[string]interface{}{"num": 2},
			Px:      map[string]string{"name": "bar"},
			Tables:  []database.Expression{database.Expr("LEFT JOIN units ON units.id = tests.id")},
			Filters: []database.Expression{database.Expr("name = 'bar4'")},
		}
		err := tx.Remove("tests", query)
		assert.Nil(t, err)
	})
}

func TestPostgresTxn_List(t *testing.T) {
	t.Parallel()

	t.Run("bad sql", func(t *testing.T) {
		tx := postgres.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.List("", database.Query{}, nil)
		assert.NotNil(t, err)
	})

	t.Run("bad exec", func(t *testing.T) {
		tx := postgres.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.List("tests", database.Query{Fields: []string{"id"}}, nil)
		assert.NotNil(t, err)
	})

	t.Run("ok for single field", func(t *testing.T) {
		tx := postgres.Txn(context.TODO())
		defer tx.Rollback()
		tx.Insert("tests", map[string]interface{}{"id": "foo1", "name": "bar1"})
		tx.Insert("tests", map[string]interface{}{"id": "list:foo2", "name": "bar2"})
		var ids []string
		err := tx.List("tests", database.Query{Eq: map[string]interface{}{"tests.name": "bar2"}, Fields: []string{"id"}}, &ids)
		assert.Nil(t, err)
		assert.Equal(t, []string{"list:foo2"}, ids)
	})

	t.Run("alias", func(t *testing.T) {
		tx := postgres.Txn(context.TODO())
		defer tx.Rollback()

		tx.Insert("tests", map[string]interface{}{"id": "alias:foo3", "name": "bar3"})
		tx.Insert("tests", map[string]interface{}{"id": "alias:foo4", "name": "bar4", "num": 1})

		var dst []map[string]interface{}
		err := tx.List("tests",
			database.Query{
				Fields: []string{"count"},
				Alias:  map[string]string{"count": "SELECT count(id) FROM units WHERE tests.id = units.id"},
			},
			&dst,
		)
		assert.Nil(t, err)
	})

	t.Run("no content", func(t *testing.T) {
		tx := postgres.Txn(context.TODO())
		defer tx.Rollback()

		var dst []map[string]interface{}
		err := tx.List("tests", database.Query{Eq: map[string]interface{}{"id": "not found"}, Fields: []string{"id"}}, &dst)
		assert.NotNil(t, err)
	})

	t.Run("uses opts", func(t *testing.T) {
		tx := postgres.Txn(context.TODO())
		defer tx.Rollback()
		tx.Insert("tests", map[string]interface{}{"id": "list:foo3", "name": "bar3"})
		tx.Insert("tests", map[string]interface{}{"id": "list:foo4", "name": "bar4", "num": 1})
		type data struct {
			Id   *string `db:"id"`
			Name *string `db:"name"`
		}
		var d []data
		query := database.Query{
			Fields:  []string{"tests.id", "tests.name"},
			NotEq:   map[string]interface{}{"name": "bar4"},
			XEq:     map[string]interface{}{"name": "%bar%"},
			Limit:   1,
			OrderBy: []string{"name"},
			Eq:      map[string]interface{}{"id": "remove:foo"},
			Gt:      map[string]interface{}{"num": 0},
			Lt:      map[string]interface{}{"num": 2},
			Px:      map[string]string{"name": "bar"},
			Tables:  []database.Expression{database.Expr("LEFT JOIN units ON units.id = tests.id")},
			Filters: []database.Expression{database.Expr("name = 'bar4'")},
		}
		err := tx.List("tests", query, &d)
		assert.NotNil(t, err)
	})
}

// NewTestPostgresDB creates a new application with preloaded testdata
func NewTestPostgresDB() (*SQL, func()) {
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

	db, err := NewSQL("postgres", databaseURL, database.Migrate(fs))
	must(err)

	return db, func() {
		s.Close()
		_ = pool.Purge(resource)
	}
}
