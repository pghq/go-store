package ark

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"
	"testing/fstest"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/pghq/go-tea/trail"
	"github.com/stretchr/testify/assert"

	"github.com/pghq/go-ark/database"
	"github.com/pghq/go-ark/database/driver"
)

var databaseURL *url.URL

func TestMain(m *testing.M) {
	trail.Testing()
	var teardown func()

	databaseURL, teardown = NewTestPostgresDB()
	defer teardown()

	os.Exit(m.Run())
}

func TestNew(t *testing.T) {
	t.Parallel()

	t.Run("bad dsn", func(t *testing.T) {
		// https://stackoverflow.com/questions/48671938/go-url-parsestring-fails-with-certain-user-names-or-passwords
		_, err := New("postgres://user:abc{DEf1=ghi@example.com:5432/db?sslmode=require")
		assert.NotNil(t, err)
	})

	t.Run("unrecognized dialect", func(t *testing.T) {
		_, err := New("mongodb://")
		assert.NotNil(t, err)
	})

	t.Run("with default db", func(t *testing.T) {
		m, err := New(databaseURL.String())
		assert.NotNil(t, m)
		assert.Nil(t, err)
	})

	t.Run("postgres", func(t *testing.T) {
		m, _ := New(databaseURL.String())
		assert.NotNil(t, m)
	})
}

func TestMapper_View(t *testing.T) {
	t.Parallel()

	m, _ := New(databaseURL.String())
	t.Run("with fn error", func(t *testing.T) {
		err := m.View(context.TODO(), func(tx Txn) error { return trail.NewError("with fn error") })
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

	m, _ := New(databaseURL.String())
	t.Run("with fn error", func(t *testing.T) {
		err := m.Do(context.TODO(), func(tx Txn) error { return trail.NewError("with fn error") })
		assert.NotNil(t, err)
	})

	t.Run("timeout", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.TODO(), 0)
		defer cancel()
		err := m.Do(ctx, func(tx Txn) error { return nil })
		assert.NotNil(t, err)
		assert.False(t, trail.IsFatal(err))
	})

	t.Run("without fn error", func(t *testing.T) {
		err := m.Do(context.TODO(), func(tx Txn) error { return nil })
		assert.Nil(t, err)
	})
}

func TestMapper_Txn(t *testing.T) {
	t.Parallel()

	m, _ := New(databaseURL.String())

	t.Run("bad commit", func(t *testing.T) {
		tx := m.Txn(context.TODO())
		tx.Insert("tests", map[string]interface{}{"id": "foo"})
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

	m, _ := New(databaseURL.String())

	t.Run("ok", func(t *testing.T) {
		tx := m.Txn(context.TODO())
		defer tx.Rollback()
		assert.Nil(t, tx.Insert("tests", map[string]interface{}{"id": "insert:foo"}))
	})
}

func TestTxn_Update(t *testing.T) {
	t.Parallel()

	m, _ := New(databaseURL.String())
	tx := m.Txn(context.TODO())
	tx.Insert("tests", map[string]interface{}{"id": "update:foo"})
	tx.Commit()

	t.Run("can update", func(t *testing.T) {
		tx := m.Txn(context.TODO())
		defer tx.Rollback()
		assert.Nil(t, tx.Update("tests", database.Query{Eq: map[string]interface{}{"id": "update:foo"}}, map[string]interface{}{"id": "update:foo"}))
	})
}

func TestTxn_Remove(t *testing.T) {
	t.Parallel()

	m, _ := New(databaseURL.String())
	tx := m.Txn(context.TODO())
	tx.Insert("tests", map[string]interface{}{"id": "remove:foo"})
	tx.Commit()

	t.Run("can remove", func(t *testing.T) {
		tx := m.Txn(context.TODO())
		defer tx.Rollback()
		assert.Nil(t, tx.Remove("tests", database.Query{Eq: map[string]interface{}{"id": "remove:foo"}}))
	})
}

func TestTxn_Get(t *testing.T) {
	t.Parallel()

	m, _ := New(databaseURL.String())
	tx := m.Txn(context.TODO())
	tx.Insert("tests", map[string]interface{}{"id": "get:foo"})
	tx.Commit()

	t.Run("not found", func(t *testing.T) {
		tx := m.Txn(context.TODO())
		defer tx.Rollback()
		var v string
		assert.NotNil(t, tx.Get("tests", database.Query{Eq: map[string]interface{}{"id": "not found"}}, document(&v)))
	})

	t.Run("can get", func(t *testing.T) {
		tx := m.Txn(context.TODO())
		defer tx.Rollback()
		var v struct {
			Id string `db:"id"`
		}
		assert.Nil(t, tx.Get("tests", database.Query{Eq: map[string]interface{}{"id": "get:foo"}}, document(&v)))
		tx.Commit()
		tx.cache.Wait()
	})

	t.Run("can get cached", func(t *testing.T) {
		tx := m.Txn(context.TODO())
		defer tx.Rollback()
		var v struct {
			Id string `db:"id"`
		}
		assert.Nil(t, tx.Get("tests", database.Query{Eq: map[string]interface{}{"id": "get:foo"}}, document(&v)))
	})
}

func TestTxn_List(t *testing.T) {
	t.Parallel()

	m, _ := New(databaseURL.String())
	tx := m.Txn(context.TODO())
	tx.Insert("tests", map[string]interface{}{"id": "list:foo"})
	tx.Commit()

	t.Run("not found", func(t *testing.T) {
		tx := m.Txn(context.TODO())
		defer tx.Rollback()
		var v []string
		assert.NotNil(t, tx.List("tests", database.Query{Eq: map[string]interface{}{"id": "not found"}}, document(&v)))
	})

	t.Run("can list", func(t *testing.T) {
		tx := m.Txn(context.TODO())
		defer tx.Rollback()
		var v []struct {
			Id string `db:"id"`
		}
		assert.Nil(t, tx.List("tests", database.Query{}, document(&v)))
		tx.Commit()
		tx.cache.Wait()
	})

	t.Run("can list cached", func(t *testing.T) {
		tx := m.Txn(context.TODO())
		defer tx.Rollback()
		var v []struct {
			Id string `db:"id"`
		}
		assert.Nil(t, tx.List("tests", database.Query{}, document(&v)))
	})
}

// NewTestPostgresDB creates a new application with preloaded testdata
func NewTestPostgresDB() (*url.URL, func()) {
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

	_, _ = driver.NewSQL("postgres", databaseURL, database.Migrate(fs))
	return databaseURL, func() {
		s.Close()
		_ = pool.Purge(resource)
	}
}

// must be nil error or panic
func must(err error) {
	if err != nil {
		panic(err)
	}
}

func document(v interface{}) DocumentDecoder {
	return documentDecoder{v: v}
}

type documentDecoder struct {
	v interface{}
}

func (d documentDecoder) Decode(fn func(v interface{}) error) error {
	return fn(d.v)
}
