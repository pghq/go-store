package driver

import (
	"database/sql"
	"net/url"
	"testing"
	"testing/fstest"

	"github.com/pghq/go-tea/trail"
	"github.com/stretchr/testify/assert"

	"github.com/pghq/go-ark/database"
)

func TestNewDB(t *testing.T) {
	t.Parallel()

	t.Run("bad open", func(t *testing.T) {
		_, err := NewSQL("postgres", &url.URL{}, database.SQLOpen(func(driverName, dataSourceName string) (*sql.DB, error) {
			return nil, trail.NewError("bad open")
		}))
		assert.NotNil(t, err)
	})

	t.Run("bad migration", func(t *testing.T) {
		_, err := NewSQL("postgres", postgres.backend.URL(), database.SQLOpen(func(driverName, dataSourceName string) (*sql.DB, error) {
			return postgres.backend.SQL(), nil
		}), database.Migrate(fstest.MapFS{
			"schema/migrations/00003_test.sql": &fstest.MapFile{
				Data: []byte("-- +goose Up\nCREATE BAD TABLE tests (id text primary key, name text, num int);"),
			},
		}))
		assert.NotNil(t, err)
	})

	t.Run("bad seed", func(t *testing.T) {
		_, err := NewSQL("postgres", postgres.backend.URL(), database.SQLOpen(func(driverName, dataSourceName string) (*sql.DB, error) {
			return postgres.backend.SQL(), nil
		}), database.Migrate(fstest.MapFS{
			"schema/migrations/00001_test.sql": &fstest.MapFile{
				Data: []byte("-- +goose Up\nCREATE TABLE IF NOT EXISTS tests (id text primary key, name text, num int);"),
			},
			"schema/seed/2/00001_test.sql": &fstest.MapFile{
				Data: []byte("-- +goose Up\nINSERT INTO tests (id) VALUES ('');"),
			},
			"schema/seed/3/00001_test.sql": &fstest.MapFile{
				Data: []byte("-- +goose Up\nINSERT INTO tests (id) VALUES (bad);"),
			},
		}))
		assert.NotNil(t, err)
	})

	t.Run("unrecognized dialect", func(t *testing.T) {
		d, err := NewSQL("", &url.URL{}, database.SQLOpen(func(driverName, dataSourceName string) (*sql.DB, error) {
			return &sql.DB{}, nil
		}))
		assert.Nil(t, d)
		assert.NotNil(t, err)
	})
}

func TestGooseLogger(t *testing.T) {
	t.Parallel()

	l := gooseLogger{}
	t.Run("print", func(t *testing.T) {
		l.Print("an error has occurred")
	})

	t.Run("printf", func(t *testing.T) {
		l.Printf("an %s has occurred", "error")
	})

	t.Run("println", func(t *testing.T) {
		l.Println("an error has occurred")
	})

	t.Run("fatal", func(t *testing.T) {
		l.Fatal("an error has occurred")
	})

	t.Run("fatalf", func(t *testing.T) {
		l.Fatalf("an %s has occurred", "error")
	})
}

func TestPlaceholder(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		s := placeholder("")
		r, err := s.ReplacePlaceholders("SELECT * FROM tests WHERE name = ?")
		assert.Nil(t, err)
		assert.Equal(t, "SELECT * FROM tests WHERE name = ?", r)
	})

	t.Run("dollar", func(t *testing.T) {
		s := placeholder("$")
		r, err := s.ReplacePlaceholders("SELECT * FROM tests WHERE name = ?")
		assert.Nil(t, err)
		assert.Equal(t, "SELECT * FROM tests WHERE name = $1", r)
	})

	t.Run("escape", func(t *testing.T) {
		s := placeholder("$")
		r, err := s.ReplacePlaceholders("SELECT * FROM tests WHERE name = ??")
		assert.Nil(t, err)
		assert.Equal(t, "SELECT * FROM tests WHERE name = ?", r)
	})
}
