package ark

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/Masterminds/squirrel"
	"github.com/alicebob/miniredis/v2"
	"github.com/pghq/go-tea"
	_ "github.com/proullon/ramsql/driver"
	"github.com/stretchr/testify/assert"

	"github.com/pghq/go-ark/internal"
)

func TestOpen(t *testing.T) {
	t.Parallel()

	t.Run("no backend", func(t *testing.T) {
		assert.NotNil(t, Open())
	})

	t.Run("dsn", func(t *testing.T) {
		dm := Open().Driver("postgres").DSN("postgres://user:pass@pg.example.com/db")
		assert.NotNil(t, dm)
	})
}

func TestMapper_ConnectKVS(t *testing.T) {
	t.Parallel()

	t.Run("bad provider", func(t *testing.T) {
		_, err := Open().ConnectKVS(context.TODO(), "bad")
		assert.NotNil(t, err)
	})

	t.Run("bad connect", func(t *testing.T) {
		_, err := Open().ConnectKVS(context.TODO(), "redis")
		assert.NotNil(t, err)
	})

	t.Run("inmem", func(t *testing.T) {
		conn, err := Open().ConnectKVS(context.TODO(), "inmem")
		assert.Nil(t, err)
		assert.NotNil(t, conn)
	})

	t.Run("redis", func(t *testing.T) {
		s, _ := miniredis.Run()
		defer s.Close()

		conn, err := Open().DSN(s.Addr()).ConnectKVS(context.TODO(), "redis", RedisConfig{Network: "tcp"})
		assert.Nil(t, err)
		assert.NotNil(t, conn)
	})
}

func TestMapper_ConnectRDB(t *testing.T) {
	t.Parallel()

	t.Run("bad provider", func(t *testing.T) {
		_, err := Open().ConnectRDB(context.TODO(), "bad")
		assert.NotNil(t, err)
	})

	t.Run("bad connect", func(t *testing.T) {
		_, err := Open().ConnectRDB(context.TODO(), "inmem")
		assert.NotNil(t, err)
	})

	t.Run("inmem", func(t *testing.T) {
		dm := Open().DSN(`{
			"tests": {
				"id": {
					"unique": true, 
					"fields": {
						"Id": "string"
					}
				}
			}
		}`)
		conn, err := dm.ConnectRDB(context.TODO(), "inmem")
		assert.Nil(t, err)
		assert.NotNil(t, conn)
	})

	t.Run("sql", func(t *testing.T) {
		db, _ := sql.Open("ramsql", "")
		defer db.Close()

		conn, err := Open().ConnectRDB(context.TODO(), "sql", SQLConfig{DB: db})
		assert.Nil(t, err)
		assert.NotNil(t, conn)
	})
}

func TestTxn(t *testing.T) {
	t.Parallel()

	dm := Open()
	_, _ = dm.ConnectKVS(context.TODO(), "inmem")

	t.Run("context timeout", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.TODO(), 0)
		defer cancel()
		_, err := dm.txn(ctx)
		assert.NotNil(t, err)
	})

	t.Run("update", func(t *testing.T) {
		tx, _ := dm.txn(context.TODO())
		ra, err := tx.update(internal.Insert{Key: []byte("test"), Value: "value"}).Resolve()
		assert.Nil(t, err)
		assert.Equal(t, 1, ra)
	})

	t.Run("view", func(t *testing.T) {
		tx, _ := dm.txn(context.TODO())
		_ = tx.update(internal.Insert{Key: []byte("test"), Value: "value"})
		tx.Commit()

		t.Run("bad commit", func(t *testing.T) {
			s, _ := miniredis.Run()
			defer s.Close()

			dm := Open().DSN(s.Addr())
			_, _ = dm.ConnectKVS(context.TODO(), "redis", RedisConfig{Network: "tcp"})

			tx, _ := dm.txn(context.TODO())
			defer tx.Rollback()
			var value string
			_ = tx.view(internal.Get{Key: []byte("not found")}, &value)
			err := tx.Commit()
			assert.NotNil(t, err)
			assert.False(t, tea.IsFatal(err))
			assert.Equal(t, "", value)
		})

		t.Run("not cached", func(t *testing.T) {
			tx, _ := dm.txn(context.TODO())
			defer tx.cache.Wait()
			defer tx.Commit()

			var value string
			ra, err := tx.view(internal.Get{Key: []byte("test")}, &value).Resolve()
			assert.Nil(t, err)
			assert.Equal(t, 1, ra)
			assert.Equal(t, "value", value)
		})

		t.Run("cached", func(t *testing.T) {
			tx, _ := dm.txn(context.TODO())
			defer tx.Commit()

			var value string
			ra, err := tx.view(internal.Get{Key: []byte("test")}, &value).Resolve()
			assert.Nil(t, err)
			assert.Equal(t, 1, ra)
			assert.Equal(t, "value", value)
		})
	})
}

func TestQy(t *testing.T) {
	t.Parallel()

	now := time.Now()
	q := Qy().
		Table("tests").
		LeftJoin("units ON units.id = ?", 1).
		After("tests.created_at", &now).
		Filter(squirrel.Eq{"tests.id": 1}).
		Fields("id").
		OrderBy("created_at").
		FieldFunc(func(s string) string {
			return fmt.Sprintf("tests.%s", s)
		}).
		First(1)

	t.Run("list sql", func(t *testing.T) {
		s, args, err := q.l.SQL("$")
		assert.Nil(t, err)
		assert.Equal(t, "SELECT tests.id FROM tests LEFT JOIN units ON units.id = $1 WHERE tests.created_at > $2 AND tests.id = $3 ORDER BY created_at LIMIT 1", s)
		assert.Equal(t, []interface{}{1, &now, 1}, args)
	})

	t.Run("get sql", func(t *testing.T) {
		s, args, err := q.get().SQL("$")
		assert.Nil(t, err)
		assert.Equal(t, "SELECT tests.id FROM tests WHERE tests.id = $1 LIMIT 1", s)
		assert.Equal(t, []interface{}{1}, args)
	})
}