package sql

import (
	"context"
	"database/sql"
	"embed"
	"io"
	"testing"

	"github.com/Masterminds/squirrel"
	"github.com/lib/pq"
	"github.com/pghq/go-tea"
	_ "github.com/proullon/ramsql/driver"
	"github.com/stretchr/testify/assert"

	"github.com/pghq/go-ark/internal"
)

func TestProvider_Connect(t *testing.T) {
	t.Parallel()

	tea.SetGlobalLogWriter(io.Discard)
	defer tea.ResetGlobalLogger()

	db, _ := sql.Open("ramsql", "TestProvider_Connect")
	defer db.Close()

	t.Run("bad primary URL", func(t *testing.T) {
		dsn := "postgres://user:abc{DEf1=ghi@pg.example.com:5432/db?sslmode=verify-ca&pool_max_conns=10"
		p := NewProvider("postgres", dsn, Config{})
		err := p.Connect(context.TODO())
		assert.NotNil(t, err)
	})

	t.Run("trace", func(t *testing.T) {
		dsn := "postgres://user:abc{DEf1=ghi@pg.example.com:5432/db?sslmode=verify-ca&pool_max_conns=10"
		p := NewProvider("postgres", dsn, Config{
			TraceDriver: pq.Driver{},
		})
		err := p.Connect(context.TODO())
		assert.NotNil(t, err)

		err = p.Connect(context.TODO())
		assert.NotNil(t, err)
	})

	t.Run("bad migration apply", func(t *testing.T) {
		p := NewProvider("", "", Config{
			DB:                 db,
			MigrationDirectory: "migrations",
			MigrationFS:        embed.FS{},
		})
		err := p.Connect(context.TODO())
		assert.NotNil(t, err)
	})

	t.Run("bad open", func(t *testing.T) {
		p := NewProvider("", "", Config{})
		p.open = func(driver, dsn string) (*sql.DB, error) {
			return nil, tea.NewError("bad open")
		}
		err := p.Connect(context.TODO())
		assert.NotNil(t, err)
	})

	t.Run("connect w/o migration", func(t *testing.T) {
		p := NewProvider("", "", Config{DB: db})
		err := p.Connect(context.TODO())
		assert.Nil(t, err)
	})
}

func TestProvider_Txn(t *testing.T) {
	t.Parallel()

	db, _ := sql.Open("ramsql", "TestProvider_Txn")
	defer db.Close()

	t.Run("bad tx begin", func(t *testing.T) {
		p := NewProvider("", "", Config{DB: db})
		_ = p.Connect(context.TODO())

		ctx, cancel := context.WithTimeout(context.TODO(), 0)
		defer cancel()
		_, err := p.Txn(ctx, true)
		assert.NotNil(t, err)
	})

	t.Run("bad read only", func(t *testing.T) {
		p := NewProvider("", "", Config{DB: db})
		_ = p.Connect(context.TODO())

		_, err := p.Txn(context.TODO(), true)
		assert.NotNil(t, err)
	})

	t.Run("write on primary", func(t *testing.T) {
		p := NewProvider("", "", Config{DB: db})
		_ = p.Connect(context.TODO())

		tx, err := p.Txn(context.TODO())
		assert.Nil(t, err)
		assert.NotNil(t, tx)
	})
}

func TestTxn_Exec(t *testing.T) {
	t.Parallel()

	db, _ := sql.Open("ramsql", "TestTxn_Exec")
	defer db.Close()

	_, _ = db.Exec("CREATE TABLE tests (test_id integer unique not null, primary key (test_id))")

	t.Run("bad statement", func(t *testing.T) {
		p := NewProvider("", "", Config{DB: db})
		_ = p.Connect(context.TODO())
		tx, _ := p.Txn(context.TODO())
		defer tx.Rollback()
		_, err := tx.Exec(internal.List{}).Resolve()
		assert.NotNil(t, err)
	})

	t.Run("missing destination", func(t *testing.T) {
		p := NewProvider("", "", Config{DB: db})
		_ = p.Connect(context.TODO())
		tx, _ := p.Txn(context.TODO())
		defer tx.Rollback()
		_, err := tx.Exec(internal.Get{Table: "tests", Fields: []interface{}{"test_id"}, Filter: Ft().Eq("test_id", 0)}).Resolve()
		assert.NotNil(t, err)
	})

	t.Run("not found", func(t *testing.T) {
		p := NewProvider("", "", Config{DB: db})
		_ = p.Connect(context.TODO())
		tx, _ := p.Txn(context.TODO())
		defer tx.Rollback()
		var dst map[string]interface{}
		_, err := tx.Exec(internal.Get{Table: "tests", Fields: []interface{}{"test_id"}, Filter: Ft().Eq("test_id", 0)}, &dst).Resolve()
		assert.NotNil(t, err)
		assert.False(t, tea.IsFatal(err))
	})

	t.Run("bad get field", func(t *testing.T) {
		p := NewProvider("", "", Config{DB: db})
		_ = p.Connect(context.TODO())
		tx, _ := p.Txn(context.TODO())
		defer tx.Rollback()
		var dst map[string]interface{}
		_, err := tx.Exec(internal.Get{Table: "tests", Fields: []interface{}{"bad"}, Filter: Ft().Eq("test_id", 0)}, &dst).Resolve()
		assert.NotNil(t, err)
		assert.True(t, tea.IsFatal(err))
	})

	t.Run("bad column value", func(t *testing.T) {
		p := NewProvider("", "", Config{DB: db})
		_ = p.Connect(context.TODO())
		tx, _ := p.Txn(context.TODO())
		defer tx.Rollback()
		_, err := tx.Exec(internal.Insert{Table: "tests", Value: map[string]interface{}{}}).Resolve()
		assert.NotNil(t, err)
	})

	t.Run("get test", func(t *testing.T) {
		p := NewProvider("", "", Config{DB: db})
		_ = p.Connect(context.TODO())
		tx, _ := p.Txn(context.TODO())
		defer tx.Rollback()
		_, _ = tx.Exec(internal.Insert{Table: "tests", Value: map[string]interface{}{
			"test_id": 0,
		}}).Resolve()

		var dst struct {
			Id int `db:"test_id"`
		}
		ra, err := tx.Exec(internal.Get{Table: "tests", Fields: []interface{}{"test_id"}, Filter: Ft().Eq("test_id", 0)}, &dst).Resolve()
		assert.Nil(t, err)
		assert.Equal(t, 1, ra)
		assert.Equal(t, 0, dst.Id)
		assert.Nil(t, tx.Commit())
	})
}

func TestFilter_ToSql(t *testing.T) {
	t.Parallel()

	t.Run("bad value", func(t *testing.T) {
		_, _, err := Ft().Lt("key", nil).ToSql()
		assert.NotNil(t, err)
	})

	t.Run("can sql-ize", func(t *testing.T) {
		or := Ft().Lt("lt", 2)
		and := Ft().Gt("gt", 3).
			NotEq("ne", 4).
			BeginsWith("prefix", "5").
			EndsWith("suffix", "6").
			Contains("containsString", "7").
			Contains("containsSlice", []interface{}{8, 9, 10}).
			Contains("containsNumber", 11).
			NotContains("notContainsString", "7").
			NotContains("notContainsSlice", []interface{}{8, 9, 10}).
			NotContains("notContainsNumber", 11).
			Expr("id = tests.id")

		f := Ft().
			Eq("eq", 1).
			Or(or).
			And(and)

		stmt, args, err := squirrel.Select("column").From("tests").Where(f).ToSql()
		assert.Nil(t, err)
		assert.Equal(t, "SELECT column FROM tests WHERE eq = ? AND (eq = ? OR lt < ?) AND (eq = ? AND (eq = ? OR lt < ?) AND gt > ? AND ne <> ? AND prefix ILIKE ? AND suffix ILIKE ? AND containsString ILIKE ? AND containsSlice IN (?,?,?) AND containsNumber IN (?) AND notContainsString NOT ILIKE ? AND notContainsSlice NOT IN (?,?,?) AND notContainsNumber NOT IN (?) AND id = tests.id)", stmt)
		assert.Equal(t, []interface{}{1, 1, 2, 1, 1, 2, 3, 4, "5%", "%6", "%7%", 8, 9, 10, 11, "%7%", 8, 9, 10, 11}, args)
	})
}
