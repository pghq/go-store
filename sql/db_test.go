package sql

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"net/url"
	"os"
	"testing"
	"testing/fstest"
	"time"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/auth"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/server"
	mysql "github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/information_schema"
	driver "github.com/go-sql-driver/mysql"
	"github.com/pghq/go-tea"
	"github.com/stretchr/testify/assert"

	"github.com/pghq/go-ark/database"
)

var d *DB

func TestMain(m *testing.M) {
	tea.Testing()
	sql.Register("tidb", driver.MySQLDriver{})
	var teardown func()
	d, teardown, _ = NewTestDB()
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
		_, teardown, err := NewTestDB(database.Migrate(embed.FS{}, "migrations", "migrations"))
		defer teardown()
		assert.NotNil(t, err)
	})

	t.Run("successful migration", func(t *testing.T) {
		fs := fstest.MapFS{
			"migrations/00001_test.sql": &fstest.MapFile{
				Data: []byte("-- +goose Up\nCREATE TABLE IF NOT EXISTS tests (id text);"),
			},
		}

		_, teardown, err := NewTestDB(database.Migrate(fs, "migrations", "migrations"))
		defer teardown()
		assert.Nil(t, err)
	})

	t.Run("with custom SQL open", func(t *testing.T) {
		d := NewDB("", &url.URL{}, database.SQLOpen(func(driverName, dataSourceName string) (*sql.DB, error) {
			return &sql.DB{}, nil
		}))
		assert.NotNil(t, d)
		assert.Nil(t, d.err)
	})

	t.Run("with custom options", func(t *testing.T) {
		_, teardown, err := NewTestDB(database.MaxConns(100), database.MaxConnLifetime(time.Minute), database.MaxIdleLifetime(time.Minute))
		defer teardown()
		assert.Nil(t, err)
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

func TestDB_Txn(t *testing.T) {
	t.Parallel()

	t.Run("write", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		assert.NotNil(t, tx)
	})

	t.Run("read only", func(t *testing.T) {
		tx := d.Txn(context.TODO(), database.ReadOnly())
		assert.NotNil(t, tx)
	})

	t.Run("can rollback", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		err := tx.Rollback()
		assert.Nil(t, err)
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

	t.Run("bad value", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Insert("tests", database.Id(""), func() {})
		assert.NotNil(t, err)
	})

	t.Run("bad sql", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Insert("", database.NamedKey(true, "foo"), map[string]interface{}{"id": "foo"})
		assert.NotNil(t, err)
	})

	t.Run("bad exec", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Insert("tests", database.NamedKey(true, "foo"), map[string]interface{}{"id": "foo", "fn": func() {}})
		assert.NotNil(t, err)
	})

	t.Run("ok", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Insert("tests", database.NamedKey(true, "foo"), map[string]interface{}{"id": "foo"})
		assert.Nil(t, err)
	})
}

func TestTxn_Update(t *testing.T) {
	t.Parallel()

	t.Run("bad value", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Update("tests", database.NamedKey(true, "foo"), func() {})
		assert.NotNil(t, err)
	})

	t.Run("bad sql", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Update("", database.NamedKey(true, "foo"), map[string]interface{}{"id": "foo"})
		assert.NotNil(t, err)
	})

	t.Run("bad exec", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Update("tests", database.NamedKey(true, "foo"), map[string]interface{}{"id": "foo", "fn": func() {}})
		assert.NotNil(t, err)
	})

	t.Run("ok", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		defer tx.Rollback()
		tx.Insert("tests", database.NamedKey(true, "foo"), map[string]interface{}{"id": "foo"})
		err := tx.Update("tests", database.NamedKey(true, "foo"), map[string]interface{}{"id": "foo"})
		assert.Nil(t, err)
	})
}

func TestTxn_Get(t *testing.T) {
	t.Parallel()

	t.Run("missing key name", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Get("tests", database.NamedKey(true, "foo"), nil)
		assert.NotNil(t, err)
	})

	t.Run("bad sql", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Get("", database.NamedKey(true, "foo"), nil)
		assert.NotNil(t, err)
	})

	t.Run("bad exec", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Get("tests", database.NamedKey(true, "foo"), nil, database.Fields("id"))
		assert.NotNil(t, err)
	})

	t.Run("not found", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		defer tx.Rollback()
		var id string
		err := tx.Get("tests", database.NamedKey(true, "not found"), &id, database.Fields("id"))
		assert.NotNil(t, err)
		assert.False(t, tea.IsFatal(err))
	})

	t.Run("ok for single field", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		defer tx.Rollback()
		tx.Insert("tests", database.NamedKey(true, "foo1"), map[string]interface{}{"id": "foo1"})
		var id string
		err := tx.Get("tests", database.NamedKey(true, "foo1"), &id, database.Fields("id"))
		assert.Nil(t, err)
		assert.Equal(t, "foo1", id)
	})

	t.Run("ok for multiple fields", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		defer tx.Rollback()
		tx.Insert("tests", database.NamedKey(true, "foo2"), map[string]interface{}{"id": "foo2", "name": "bar"})
		type data struct {
			Id   *string `db:"id"`
			Name *string `db:"name"`
		}
		var d data
		err := tx.Get("tests", database.NamedKey(true, "foo2"), &d, database.Fields("id", "name"))
		assert.Nil(t, err)
		assert.Equal(t, "foo2", *d.Id)
		assert.Equal(t, "bar2", *d.Name)
	})
}

func TestTxn_Remove(t *testing.T) {
	t.Parallel()

	t.Run("bad sql", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.Remove("", database.NamedKey(true, "foo"))
		assert.NotNil(t, err)
	})

	t.Run("ok", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		defer tx.Rollback()
		tx.Insert("tests", database.NamedKey(true, "foo"), map[string]interface{}{"id": "foo"})
		err := tx.Remove("tests", database.NamedKey(true, "foo"))
		assert.Nil(t, err)
	})
}

func TestTxn_List(t *testing.T) {
	t.Parallel()

	t.Run("bad sql", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.List("", nil)
		assert.NotNil(t, err)
	})

	t.Run("bad exec", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		defer tx.Rollback()
		err := tx.List("tests", nil, database.Fields("id"))
		assert.NotNil(t, err)
	})

	t.Run("ok for single field", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		defer tx.Rollback()
		tx.Insert("tests", database.NamedKey(true, "foo1"), map[string]interface{}{"id": "foo1", "name": "bar1"})
		tx.Insert("tests", database.NamedKey(true, "foo2"), map[string]interface{}{"id": "foo2", "name": "bar2"})
		var ids []string
		err := tx.List("tests", &ids, database.Eq("tests.name", "bar2"), database.Fields("id"))
		assert.Nil(t, err)
		assert.Equal(t, []string{"foo2"}, ids)
	})

	t.Run("uses opts", func(t *testing.T) {
		tx := d.Txn(context.TODO())
		defer tx.Rollback()
		tx.Insert("tests", database.NamedKey(true, "foo3"), map[string]interface{}{"id": "foo3", "name": "bar3"})
		tx.Insert("tests", database.NamedKey(true, "foo4"), map[string]interface{}{"id": "foo4", "name": "bar4", "num": 1})
		type data struct {
			Id   *string `db:"id"`
			Name *string `db:"name"`
		}
		var d []data
		opts := []database.QueryOption{
			database.Eq("name", "bar4"),
			database.NotEq("name", "bar4"),
			database.Fields("tests.id", "tests.name"),
			database.XEq("name", "%bar%"),
			database.Limit(1),
			database.OrderBy("name"),
			database.Gt("num", 0),
			database.Lt("num", 2),
			database.Table("LEFT JOIN units ON units.id = tests.id"),
			database.Filter("name = 'bar4'"),
		}
		err := tx.List("tests", &d, opts...)
		assert.Nil(t, err)
	})
}

// NewTestDB creates a new test database
func NewTestDB(opts ...database.Option) (*DB, func(), error) {
	config := server.Config{
		Protocol: "tcp",
		Address:  ":0",
		Auth:     auth.NewNativeSingle("user", "pass", auth.AllPermissions),
	}
	engine := sqle.NewDefault(mysql.NewDatabaseProvider(memory.NewDatabase("db"), information_schema.NewInformationSchemaDatabase()))
	s, err := server.NewDefaultServer(config, engine)
	must(err)
	go s.Start()

	dsn := fmt.Sprintf("user:pass@tcp(%s)/db", s.Listener.Addr())
	fs := fstest.MapFS{
		"migrations/00001_test.sql": &fstest.MapFile{
			Data: []byte("-- +goose Up\nCREATE TABLE IF NOT EXISTS tests (id text, name text, num int);\nCREATE TABLE IF NOT EXISTS units (id text);"),
		},
	}

	databaseURL, _ := url.Parse(dsn)
	opts = append([]database.Option{database.Migrate(fs, "migrations", "migrations")}, opts...)
	d := NewDB("tidb", databaseURL, opts...)
	return d, func() { must(s.Close()) }, d.Ping(context.TODO())
}

// must be nil error or panic
func must(err error) {
	if err != nil {
		panic(err)
	}
}
