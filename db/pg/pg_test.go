package pg

import (
	"context"
	"os"
	"testing"
	"testing/fstest"
	"time"

	"github.com/pghq/go-store/db"
	"github.com/pghq/go-store/db/pg/pgtest"

	"github.com/pghq/go-tea/trail"

	"github.com/stretchr/testify/assert"
)

var (
	dsn string
	pg  *Provider
)

func TestMain(m *testing.M) {
	trail.Testing()
	var cleanup func() error
	var err error
	dsn, cleanup, err = pgtest.Start()
	if err != nil {
		panic(err)
	}

	pg, err = New(dsn, fstest.MapFS{
		"migrations/00001_test.sql": &fstest.MapFile{
			Data: []byte("-- +goose Up\nCREATE TABLE tests (id text primary key, name text, num int); \n create index idx_tests_name ON tests (name);"),
		},
	})
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

	t.Run("parse config error", func(t *testing.T) {
		_, err := New(":memory:", nil)
		assert.NotNil(t, err)
	})

	t.Run("connect config error", func(t *testing.T) {
		_, err := New(dsn, nil, WithConnectTimeout(0))
		assert.NotNil(t, err)
	})

	t.Run("bad migration", func(t *testing.T) {
		_, err := New(dsn, fstest.MapFS{})
		assert.NotNil(t, err)
	})

	t.Run("ok", func(t *testing.T) {
		p, _ := New(dsn, nil,
			WithMaxConns(100),
			WithMaxConnLifetime(time.Second),
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

		_, err := pg.Begin(ctx)
		assert.NotNil(t, err)
	})

	t.Run("ok", func(t *testing.T) {
		uow, err := pg.Begin(context.TODO(), db.WithReadOnly(true))
		assert.Nil(t, err)
		assert.NotNil(t, uow)
		assert.NotNil(t, uow.Tx())
		defer uow.Rollback(context.TODO())
		assert.Nil(t, uow.Commit(context.TODO()))

		ctx := db.WithUnitOfWork(context.TODO(), uow)
		assert.NotNil(t, pg.Repository().(repository).conn(ctx))
	})
}

func TestProvider_Repository(t *testing.T) {
	trail.Testing()
	t.Parallel()

	t.Run("ok", func(t *testing.T) {
		assert.NotNil(t, pg.Repository())
	})
}
