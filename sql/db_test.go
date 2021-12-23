package sql

import (
	"context"
	"database/sql"
	"embed"
	"net/url"
	"os"
	"testing"

	"github.com/pghq/go-tea"
	"github.com/stretchr/testify/assert"

	"github.com/pghq/go-ark/database"
)

func TestMain(m *testing.M) {
	tea.Testing()
	var teardown func()

	postgres, teardown = NewTestPostgresDB()
	defer teardown()

	os.Exit(m.Run())
}

func TestNewDB(t *testing.T) {
	t.Parallel()

	t.Run("bad open", func(t *testing.T) {
		d := NewDB("", &url.URL{}, database.SQLOpen(func(driverName, dataSourceName string) (*sql.DB, error) {
			return nil, tea.Err("bad open")
		}))
		assert.NotNil(t, d.Ping(context.TODO()))
		assert.NotNil(t, d.Txn(context.TODO()).Commit())
		assert.NotNil(t, d.Txn(context.TODO()).Rollback())
		assert.NotNil(t, d.Txn(context.TODO()).Get("", database.Id(""), nil))
		assert.NotNil(t, d.Txn(context.TODO()).Insert("", database.Id(""), nil))
		assert.NotNil(t, d.Txn(context.TODO()).Remove("", database.Id("")))
		assert.NotNil(t, d.Txn(context.TODO()).Update("", database.Id(""), nil))
		assert.NotNil(t, d.Txn(context.TODO()).List("", ""))
	})

	t.Run("bad migration", func(t *testing.T) {
		db := NewDB("postgres", postgres.backend.URL(), database.SQLOpen(func(driverName, dataSourceName string) (*sql.DB, error) {
			return postgres.backend.SQL(), nil
		}), database.Migrate(embed.FS{}, "migrations", "migrations"))
		assert.NotNil(t, db.err)
	})

	t.Run("unrecognized dialect", func(t *testing.T) {
		d := NewDB("", &url.URL{}, database.SQLOpen(func(driverName, dataSourceName string) (*sql.DB, error) {
			return &sql.DB{}, nil
		}))
		assert.NotNil(t, d)
		assert.NotNil(t, d.err)
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

// must be nil error or panic
func must(err error) {
	if err != nil {
		panic(err)
	}
}
