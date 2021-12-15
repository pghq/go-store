package sql

import (
	"bytes"
	"context"
	"database/sql"
	"embed"
	"strings"
	"testing"
	"time"

	"github.com/pghq/go-tea"
	_ "github.com/proullon/ramsql/driver"
	ramsql "github.com/proullon/ramsql/driver"
	"github.com/stretchr/testify/assert"

	"github.com/pghq/go-ark/db"
)

func TestNewDB(t *testing.T) {
	t.Parallel()

	t.Run("bad open", func(t *testing.T) {
		d := NewDB(db.SQLOpen(func(driverName, dataSourceName string) (*sql.DB, error) {
			return nil, tea.NewError("bad open")
		}))
		assert.NotNil(t, d.Ping(context.TODO()))
		assert.NotNil(t, d.Txn(context.TODO()).Commit())
		assert.NotNil(t, d.Txn(context.TODO()).Rollback())
		assert.NotNil(t, d.Txn(context.TODO()).Get("", "", nil))
		assert.NotNil(t, d.Txn(context.TODO()).Insert("", "", nil))
		assert.NotNil(t, d.Txn(context.TODO()).Remove("", ""))
		assert.NotNil(t, d.Txn(context.TODO()).Update("", "", nil))
		assert.NotNil(t, d.Txn(context.TODO()).List("", ""))
	})

	sdb, _ := sql.Open("ramsql", ":inmemory1:")
	defer sdb.Close()

	t.Run("bad migration", func(t *testing.T) {
		d := NewDB(db.SQL(sdb), db.Migration(embed.FS{}, "migrations", "migrations"))
		assert.NotNil(t, d.err)
	})

	t.Run("trace", func(t *testing.T) {
		d := NewDB(db.DriverName("ramsql"), db.DSN("trace"), db.SQLTrace(ramsql.NewDriver()))
		assert.Nil(t, d.err)
	})

	t.Run("trace again", func(t *testing.T) {
		d := NewDB(db.DriverName("ramsql"), db.DSN("trace"), db.SQLTrace(ramsql.NewDriver()))
		assert.Nil(t, d.err)
		d.Ping(context.TODO())
	})

	t.Run("with custom SQL open", func(t *testing.T) {
		d := NewDB(db.SQLOpen(func(driverName, dataSourceName string) (*sql.DB, error) {
			return &sql.DB{}, nil
		}))
		assert.NotNil(t, d)
		assert.Nil(t, d.err)
	})

	t.Run("with custom SQL db", func(t *testing.T) {
		d := NewDB(db.SQL(sdb), db.MaxConns(100), db.MaxConnLifetime(time.Minute), db.MaxIdleLifetime(time.Minute))
		assert.NotNil(t, d)
		assert.Nil(t, d.Ping(context.TODO()))
	})
}

func TestGooseLogger(t *testing.T) {
	t.Parallel()

	l := gooseLogger{}
	var buf bytes.Buffer
	tea.SetGlobalLogWriter(&buf)
	defer tea.ResetGlobalLogger()
	tea.SetGlobalLogLevel("info")

	t.Run("print", func(t *testing.T) {
		buf.Reset()
		l.Print("an error has occurred")
		assert.True(t, strings.Contains(buf.String(), "an error has occurred"))
	})

	t.Run("printf", func(t *testing.T) {
		buf.Reset()
		l.Printf("an %s has occurred", "error")
		assert.True(t, strings.Contains(buf.String(), "an error has occurred"))
	})

	t.Run("println", func(t *testing.T) {
		buf.Reset()
		l.Println("an error has occurred")
		assert.True(t, strings.Contains(buf.String(), "an error has occurred"))
	})

	t.Run("fatal", func(t *testing.T) {
		buf.Reset()
		l.Fatal("an error has occurred")
		assert.True(t, strings.Contains(buf.String(), "an error has occurred"))
	})

	t.Run("fatalf", func(t *testing.T) {
		buf.Reset()
		l.Fatalf("an %s has occurred", "error")
		assert.True(t, strings.Contains(buf.String(), "an error has occurred"))
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

func TestDB_Txn(t *testing.T) {
	t.Parallel()

	sdb, _ := sql.Open("ramsql", ":inmemory2:")
	defer sdb.Close()

	d := NewDB(db.SQL(sdb))

	t.Run("write", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		assert.NotNil(t, tx)
	})

	t.Run("read only", func(t *testing.T) {
		tx := d.Txn(context.TODO(), db.ReadOnly())
		assert.NotNil(t, tx)
	})

	t.Run("can rollback", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		err := tx.Rollback()
		assert.NotNil(t, err)
	})

	t.Run("can commit", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		assert.NotNil(t, tx)
		err := tx.Commit()
		assert.Nil(t, err)
	})
}

func TestTxn_Insert(t *testing.T) {
	t.Parallel()

	sdb, _ := sql.Open("ramsql", ":inmemory3:")
	defer sdb.Close()

	sdb.Exec("CREATE TABLE tests (id TEXT)")

	d := NewDB(db.SQL(sdb))

	t.Run("missing key name", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Insert("tests", "foo", map[string]interface{}{"id": "foo"})
		assert.NotNil(t, err)
	})

	t.Run("bad value", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Insert("tests", "foo", func() {}, db.CommandKey("id"))
		assert.NotNil(t, err)
	})

	t.Run("bad sql", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Insert("", "foo", map[string]interface{}{"id": "foo"}, db.CommandKey("id"))
		assert.NotNil(t, err)
	})

	t.Run("bad exec", func(t *testing.T) {
		// XXX: https://github.com/proullon/ramsql/issues/55
		sdb, _ := sql.Open("ramsql", ":inmemory4:")
		defer sdb.Close()
		d := NewDB(db.SQL(sdb))
		tx := d.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Insert("tests", "foo", map[string]interface{}{"id": "foo", "fn": func() {}}, db.CommandKey("id"))
		assert.NotNil(t, err)
	})

	t.Run("ok", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Insert("tests", "foo", map[string]interface{}{"id": "foo"},
			db.CommandKey("id"),
			db.CommandSQLPlaceholder("?"),
		)
		assert.Nil(t, err)
	})
}

func TestTxn_Update(t *testing.T) {
	t.Parallel()

	sdb, _ := sql.Open("ramsql", ":inmemory5:")
	defer sdb.Close()

	sdb.Exec("CREATE TABLE tests (id TEXT)")

	d := NewDB(db.SQL(sdb))

	t.Run("missing key name", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Update("tests", "foo", map[string]interface{}{"id": "foo"})
		assert.NotNil(t, err)
	})

	t.Run("bad value", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Update("tests", "foo", func() {}, db.CommandKey("id"))
		assert.NotNil(t, err)
	})

	t.Run("bad sql", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Update("", "foo", map[string]interface{}{"id": "foo"}, db.CommandKey("id"))
		assert.NotNil(t, err)
	})

	t.Run("bad exec", func(t *testing.T) {
		// XXX: https://github.com/proullon/ramsql/issues/55
		sdb, _ := sql.Open("ramsql", ":inmemory6:")
		defer sdb.Close()
		d := NewDB(db.SQL(sdb))
		tx := d.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Update("tests", "foo", map[string]interface{}{"id": "foo", "fn": func() {}}, db.CommandKey("id"))
		assert.NotNil(t, err)
	})

	t.Run("ok", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		defer tx.Rollback()
		tx.Insert("tests", "id", map[string]interface{}{"id": "foo"}, db.CommandKey("id"))
		err := tx.Update("tests", "foo", map[string]interface{}{"id": "foo"}, db.CommandKey("id"))
		assert.Nil(t, err)
	})
}

func TestTxn_Get(t *testing.T) {
	t.Parallel()

	sdb, _ := sql.Open("ramsql", ":inmemory7:")
	defer sdb.Close()

	sdb.Exec("CREATE TABLE tests (id TEXT, name TEXT)")

	d := NewDB(db.SQL(sdb))

	t.Run("missing key name", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Get("tests", "foo", nil)
		assert.NotNil(t, err)
	})

	t.Run("bad sql", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Get("", "foo", nil, db.QueryKey("id"))
		assert.NotNil(t, err)
	})

	t.Run("bad exec", func(t *testing.T) {
		// XXX: https://github.com/proullon/ramsql/issues/55
		sdb, _ := sql.Open("ramsql", ":inmemory8:")
		defer sdb.Close()
		d := NewDB(db.SQL(sdb))
		tx := d.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Get("tests", "foo", nil, db.QueryKey("id"), db.Fields("id"))
		assert.NotNil(t, err)
	})

	t.Run("not found", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		defer tx.Rollback()
		var id string
		err := tx.Get("tests", "not found", &id, db.QueryKey("id"), db.Fields("id"))
		assert.NotNil(t, err)
		assert.False(t, tea.IsFatal(err))
	})

	t.Run("ok for single field", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		defer tx.Rollback()
		tx.Insert("tests", "foo1", map[string]interface{}{"id": "foo1"}, db.CommandKey("id"))
		var id string
		err := tx.Get("tests", "foo1", &id, db.QueryKey("id"), db.Fields("id"))
		assert.Nil(t, err)
		assert.Equal(t, "foo1", id)
	})

	t.Run("ok for multiple fields", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		defer tx.Rollback()
		tx.Insert("tests", "foo2", map[string]interface{}{"id": "foo2", "name": "bar"}, db.CommandKey("id"))
		type data struct {
			Id   *string `db:"id"`
			Name *string `db:"name"`
		}
		var d data
		err := tx.Get("tests", "foo2", &d, db.QueryKey("id"), db.Fields("id", "name"))
		assert.Nil(t, err)
		assert.Equal(t, "foo2", *d.Id)
		assert.Equal(t, "bar", *d.Name)
	})
}

func TestTxn_Remove(t *testing.T) {
	t.Parallel()

	sdb, _ := sql.Open("ramsql", ":inmemory9:")
	defer sdb.Close()

	sdb.Exec("CREATE TABLE tests (id TEXT)")

	d := NewDB(db.SQL(sdb))

	t.Run("missing key name", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Remove("tests", "foo")
		assert.NotNil(t, err)
	})

	t.Run("bad sql", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Remove("", "foo", db.CommandKey("id"))
		assert.NotNil(t, err)
	})

	t.Run("bad exec", func(t *testing.T) {
		// XXX: https://github.com/proullon/ramsql/issues/55
		sdb, _ := sql.Open("ramsql", ":inmemory10:")
		defer sdb.Close()
		d := NewDB(db.SQL(sdb))
		tx := d.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Remove("tests", "foo", db.CommandKey("id"))
		assert.NotNil(t, err)
	})

	t.Run("ok", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		defer tx.Rollback()
		tx.Insert("tests", "foo", map[string]interface{}{"id": "foo"}, db.CommandKey("id"))
		err := tx.Remove("tests", "foo", db.CommandKey("id"))
		assert.Nil(t, err)
	})
}

func TestTxn_List(t *testing.T) {
	t.Parallel()

	sdb, _ := sql.Open("ramsql", ":inmemory11:")
	defer sdb.Close()

	sdb.Exec("CREATE TABLE tests (id TEXT, name TEXT, num INT)")

	d := NewDB(db.SQL(sdb))

	t.Run("bad sql", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.List("", nil, db.QueryKey("id"))
		assert.NotNil(t, err)
	})

	t.Run("bad exec", func(t *testing.T) {
		// XXX: https://github.com/proullon/ramsql/issues/55
		sdb, _ := sql.Open("ramsql", ":inmemory12:")
		defer sdb.Close()
		d := NewDB(db.SQL(sdb))
		tx := d.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.List("tests", nil, db.QueryKey("id"), db.Fields("id"))
		assert.NotNil(t, err)
	})

	t.Run("ok for single field", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		defer tx.Rollback()
		tx.Insert("tests", "foo1", map[string]interface{}{"id": "foo1", "name": "bar1"}, db.CommandKey("id"))
		tx.Insert("tests", "foo2", map[string]interface{}{"id": "foo2", "name": "bar2"}, db.CommandKey("id"))
		var ids []string
		err := tx.List("tests", &ids, db.Eq("name", "bar2"), db.Fields("id"))
		assert.Nil(t, err)
		assert.Equal(t, []string{"foo2"}, ids)
	})

	t.Run("uses opts", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		defer tx.Rollback()
		tx.Insert("tests", "foo3", map[string]interface{}{"id": "foo3", "name": "bar3"}, db.CommandKey("id"))
		tx.Insert("tests", "foo4", map[string]interface{}{"id": "foo4", "name": "bar4", "num": 1}, db.CommandKey("id"))
		type data struct {
			Id   *string `db:"id"`
			Name *string `db:"name"`
		}
		var d []data
		opts := []db.QueryOption{
			db.Eq("name", "bar4"),
			db.NotEq("name", "bar4"),
			db.Fields("id", "name"),
			db.XEq("name", "%bar%"),
			db.NotXEq("id", "foo3"),
			db.Limit(1),
			db.OrderBy("name"),
			db.Gt("num", 0),
			db.Lt("num", 2),
			db.Expr("name = 'bar4'"),
			db.QuerySQLPlaceholder("?"),
		}
		err := tx.List("tests", &d, opts...)

		// XXX: ramsql driver does not support LIKE operator
		assert.NotNil(t, err)
	})
}
