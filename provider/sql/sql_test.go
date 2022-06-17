package sql

import (
	"context"
	"os"
	"testing"
	"testing/fstest"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/pghq/go-tea/trail"
	"github.com/stretchr/testify/assert"

	"github.com/pghq/go-store/provider"
)

var prov *Provider

func TestMain(m *testing.M) {
	trail.Testing()
	prov, _ = New("sqlite3", "file:provider.db?mode=memory&cache=shared", fstest.MapFS{
		"migrations/00001_test.sql": &fstest.MapFile{
			Data: []byte("-- +goose Up\nCREATE TABLE tests (id text primary key, name text, num int); create index idx_tests_name ON tests (name);"),
		},
	})
	os.Exit(m.Run())
}

func TestNew(t *testing.T) {
	trail.Testing()
	t.Parallel()

	t.Run("bad dsn", func(t *testing.T) {
		// https://stackoverflow.com/questions/48671938/go-url-parsestring-fails-with-certain-user-names-or-passwords
		_, err := New("sqlite3", "sql://user:abc{DEf1=ghi@example.com:5432/db?sslmode=require", nil)
		assert.NotNil(t, err)
	})

	t.Run("bad migration", func(t *testing.T) {
		_, err := New("sqlite3", ":memory:", fstest.MapFS{})
		assert.NotNil(t, err)
	})

	t.Run("ok", func(t *testing.T) {
		p, _ := New("sqlite3", ":memory:", nil,
			WithMaxOpenConns(100),
			WithMaxIdleConns(100),
			WithConnMaxLifetime(time.Second),
			WithConnMaxIdleTime(time.Second),
		)
		assert.NotNil(t, p)
	})
}

func TestProvider_Begin(t *testing.T) {
	trail.Testing()
	t.Parallel()

	t.Run("bad context", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.TODO(), 1)
		defer cancel()

		_, err := prov.Begin(ctx)
		assert.NotNil(t, err)
	})

	t.Run("ok", func(t *testing.T) {
		uow, err := prov.Begin(context.TODO(), provider.WithReadOnly(false))
		defer uow.Rollback(context.TODO())
		assert.Nil(t, err)
		assert.NotNil(t, uow)
		assert.Nil(t, uow.Commit(context.TODO()))
	})
}

func TestProvider_Repository(t *testing.T) {
	trail.Testing()
	t.Parallel()

	repo := prov.Repository()

	t.Run("add", func(t *testing.T) {
		t.Run("bad data encode", func(t *testing.T) {
			assert.NotNil(t, repo.Add(context.TODO(), "tests", func() {}))
		})

		t.Run("bad sql", func(t *testing.T) {
			assert.NotNil(t, repo.Add(context.TODO(), "", nil))
		})

		t.Run("ok", func(t *testing.T) {
			assert.Nil(t, repo.Add(context.TODO(), "tests", map[string]interface{}{"id": "1234"}))
		})
	})

	t.Run("edit", func(t *testing.T) {
		t.Run("bad data encode", func(t *testing.T) {
			assert.NotNil(t, repo.Edit(context.TODO(), "", spec(""), func() {}))
		})

		t.Run("bad sql", func(t *testing.T) {
			assert.NotNil(t, repo.Edit(context.TODO(), "", spec(""), nil))
		})

		t.Run("ok", func(t *testing.T) {
			assert.Nil(t, repo.Edit(context.TODO(), "tests", spec("id = 'edit:1234'"), map[string]interface{}{"id": "1234"}))
		})
	})

	t.Run("first", func(t *testing.T) {
		t.Run("bad sql", func(t *testing.T) {
			assert.NotNil(t, repo.First(context.TODO(), spec(""), nil))
		})

		t.Run("ok", func(t *testing.T) {
			var v struct{ Id string }
			assert.Nil(t, repo.First(context.TODO(), spec("SELECT id FROM tests WHERE id = '1234'"), &v))
			assert.NotEqual(t, "", v.Id)
		})
	})

	t.Run("list", func(t *testing.T) {
		t.Run("bad sql", func(t *testing.T) {
			assert.NotNil(t, repo.List(context.TODO(), spec(""), nil))
		})

		t.Run("ok", func(t *testing.T) {
			var v []struct{ Id string }
			assert.Nil(t, repo.List(context.TODO(), spec("SELECT id FROM tests WHERE id = '1234'"), &v))
			assert.NotEmpty(t, v)
		})
	})

	t.Run("remove", func(t *testing.T) {
		t.Run("bad sql", func(t *testing.T) {
			assert.NotNil(t, repo.Remove(context.TODO(), "", spec("")))
		})

		t.Run("ok", func(t *testing.T) {
			assert.Nil(t, repo.Remove(context.TODO(), "tests", spec("id = 'edit:1234'")))
		})
	})
}

type spec string

func (s spec) Id() interface{} {
	return string(s)
}

func (s spec) ToSql() (string, []interface{}, error) {
	if s == "" {
		return "", nil, trail.NewError("bad SQL statement")
	}

	return string(s), nil, nil
}
