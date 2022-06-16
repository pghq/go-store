package sql

import (
	"context"
	"testing"
	"testing/fstest"

	_ "github.com/mattn/go-sqlite3"
	"github.com/pghq/go-tea/trail"
	"github.com/stretchr/testify/assert"

	"github.com/pghq/go-store/provider"
)

func TestNew(t *testing.T) {
	trail.Testing()
	t.Parallel()

	t.Run("bad dsn", func(t *testing.T) {
		// https://stackoverflow.com/questions/48671938/go-url-parsestring-fails-with-certain-user-names-or-passwords
		_, err := New("sqlite3", "sql://user:abc{DEf1=ghi@example.com:5432/db?sslmode=require")
		assert.NotNil(t, err)
	})

	t.Run("bad migration", func(t *testing.T) {
		_, err := New("sqlite3", ":memory:", WithMigration(fstest.MapFS{}))
		assert.NotNil(t, err)
	})

	t.Run("ok", func(t *testing.T) {
		p, _ := New("sqlite3", ":memory:", WithMigration(nil))
		assert.NotNil(t, p)
	})
}

func TestProvider_Begin(t *testing.T) {
	trail.Testing()
	t.Parallel()

	p, _ := New("sqlite3", "file:begin?mode=memory&cache=shared", WithMigration(fstest.MapFS{
		"migrations/00001_test.sql": &fstest.MapFile{
			Data: []byte("-- +goose Up\nCREATE TABLE tests (id text primary key, name text, num int); create index idx_tests_name ON tests (name);"),
		},
	}))
	t.Run("bad context", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.TODO(), 1)
		defer cancel()

		_, err := p.Begin(ctx)
		assert.NotNil(t, err)
	})

	t.Run("ok", func(t *testing.T) {
		uow, err := p.Begin(context.TODO(), provider.WithReadOnly(false))
		assert.Nil(t, err)
		assert.NotNil(t, uow)
	})
}

func TestProvider_Repository(t *testing.T) {
	trail.Testing()
	t.Parallel()

	p, _ := New("sqlite3", "file:repo?mode=memory&cache=shared", WithMigration(fstest.MapFS{
		"migrations/00001_test.sql": &fstest.MapFile{
			Data: []byte("-- +goose Up\nCREATE TABLE tests (id text primary key, name text, num int); create index idx_tests_name ON tests (name);"),
		},
	}))
	repo := p.Repository()

	t.Run("add", func(t *testing.T) {
		t.Run("bad data encode", func(t *testing.T) {
			assert.NotNil(t, repo.Add(context.TODO(), "tests", func() {}))
		})

		t.Run("bad sql", func(t *testing.T) {
			assert.NotNil(t, repo.Add(context.TODO(), "tests", nil))
		})

		t.Run("ok", func(t *testing.T) {
			assert.Nil(t, repo.Add(context.TODO(), "tests", map[string]interface{}{"id": "1234"}))
		})
	})

	t.Run("first", func(t *testing.T) {
		t.Run("bad sql", func(t *testing.T) {
			assert.NotNil(t, repo.First(context.TODO(), spec(""), nil))
		})

		t.Run("ok", func(t *testing.T) {
			var v struct{ Id string }
			assert.Nil(t, repo.First(context.TODO(), spec("SELECT id FROM tests WHERE id = '1234'"), &v))
		})
	})

	t.Run("ok", func(t *testing.T) {
		uow, err := p.Begin(context.TODO(), provider.WithReadOnly(false))
		assert.Nil(t, err)
		assert.NotNil(t, uow)
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
