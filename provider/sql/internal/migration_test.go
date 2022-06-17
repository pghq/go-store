package internal

import (
	"database/sql"
	"testing"
	"testing/fstest"

	_ "github.com/mattn/go-sqlite3"
	"github.com/pghq/go-tea/trail"
	"github.com/stretchr/testify/assert"
)

func TestApply(t *testing.T) {
	trail.Testing()
	t.Parallel()

	t.Run("bad migration", func(t *testing.T) {
		assert.NotNil(t, Apply(Config{}))
	})

	t.Run("ok", func(t *testing.T) {
		db, _ := sql.Open("sqlite3", ":memory:")
		assert.Nil(t, Apply(Config{
			Dialect: "sqlite3",
			DB:      db,
			FS: fstest.MapFS{
				"migrations/00001_test.sql": &fstest.MapFile{
					Data: []byte("-- +goose Up\nCREATE TABLE tests (id text primary key, name text, num int); create index idx_tests_name ON tests (name);"),
				},
			},
		}))
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
