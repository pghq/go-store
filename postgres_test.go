package ark

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"embed"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/Masterminds/squirrel"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pghq/go-tea"
	"github.com/stretchr/testify/assert"

	"github.com/pghq/go-ark/internal/mock"
)

var (
	_ pgPool   = &PostgresPool{}
	_ pgx.Tx   = &PostgresTx{}
	_ pgx.Rows = &PostgresRows{}
)

func TestPostgres(t *testing.T) {
	t.Run("can detect postgres", func(t *testing.T) {
		tea.SetGlobalLogWriter(io.Discard)
		defer tea.ResetGlobalLogger()
		dsn := "postgres://postgres:postgres@db:5432"
		_, err := NewStore(dsn)
		assert.NotNil(t, err)
	})

	t.Run("can set store options", func(t *testing.T) {
		client := pgClient{
			conf: Config{
				primary:   "postgres://postgres:postgres@db:5432",
				secondary: "postgres://postgres:postgres@db:5432",
			},
			connect: func(ctx context.Context, config *pgxpool.Config) (*pgxpool.Pool, error) {
				return &pgxpool.Pool{}, nil
			},
		}
		opts := []Option{
			RawClient(&client),
			MaxConnLifetime(time.Second),
			MaxConns(5),
			Secondary("postgres://postgres:postgres@db:5432"),
			MigrationFS(embed.FS{}),
			MigrationDirectory("migrations"),
		}

		store, err := NewStore("postgres://postgres:postgres@db:5432", opts...)
		assert.Nil(t, err)
		assert.NotNil(t, store)
	})

	t.Run("raises connect errors", func(t *testing.T) {
		tea.SetGlobalLogWriter(io.Discard)
		defer tea.ResetGlobalLogger()
		_, err := NewStore("")
		assert.NotNil(t, err)
	})

	t.Run("bad primary", func(t *testing.T) {
		client := pgClient{
			conf: Config{
				primary:   "postgres://postgres:postgres@db:5432",
				secondary: "postgres://postgres:postgres@db:5432",
			},
			connect: func(ctx context.Context, config *pgxpool.Config) (*pgxpool.Pool, error) {
				return nil, tea.NewError("bad primary")
			},
		}

		_, err := NewStore("", RawClient(&client))
		assert.NotNil(t, err)
	})

	t.Run("can not set a bad secondary", func(t *testing.T) {
		client := pgClient{
			conf: Config{
				primary:   "postgres://postgres:postgres@db:5432",
				secondary: "secondary",
			},
			connect: func(ctx context.Context, config *pgxpool.Config) (*pgxpool.Pool, error) {
				if config.ConnString() == "secondary" {
					return nil, tea.NewError("bad secondary")
				}
				return &pgxpool.Pool{}, nil
			},
		}
		_, err := NewStore("", RawClient(&client))
		assert.NotNil(t, err)
	})

	t.Run("raises sql open errors on migration", func(t *testing.T) {
		client := pgClient{
			conf: Config{
				primary:            "postgres://postgres:postgres@db:5432",
				secondary:          "postgres://postgres:postgres@db:5432",
				migrationFS:        embed.FS{},
				migrationDirectory: "migrations",
			},
			connect: func(ctx context.Context, config *pgxpool.Config) (*pgxpool.Pool, error) {
				return &pgxpool.Pool{}, nil
			},
			open: func(driverName, dataSourceName string) (*sql.DB, error) {
				return &sql.DB{}, tea.NewError("an error has occurred")
			},
		}

		_, err := NewStore("", RawClient(&client))
		assert.NotNil(t, err)
	})

	t.Run("raises migration errors", func(t *testing.T) {
		client := pgClient{
			conf: Config{
				primary:            "postgres://postgres:postgres@db:5432",
				secondary:          "postgres://postgres:postgres@db:5432",
				migrationFS:        embed.FS{},
				migrationDirectory: "migrations",
			},
			connect: func(ctx context.Context, config *pgxpool.Config) (*pgxpool.Pool, error) {
				return &pgxpool.Pool{}, nil
			},
			open: func(driverName, dataSourceName string) (*sql.DB, error) {
				return sql.OpenDB(ErrConnector{}), nil
			},
		}

		_, err := NewStore("", RawClient(&client))
		assert.NotNil(t, err)
	})

	t.Run("can recognize integrity violation errors", func(t *testing.T) {
		err := &pgconn.PgError{Code: pgerrcode.IntegrityConstraintViolation}
		assert.True(t, IsPgIntegrityConstraintViolation(err))
	})

	t.Run("can distinguish non integrity violation errors", func(t *testing.T) {
		err := tea.NewError("an error has occurred")
		assert.False(t, IsPgIntegrityConstraintViolation(err))
	})

	t.Run("can send pgx logs", func(t *testing.T) {
		l := pgxLogger{}
		var buf bytes.Buffer
		tea.SetGlobalLogWriter(&buf)
		defer tea.ResetGlobalLogger()

		tea.SetGlobalLogLevel("debug")
		l.Log(context.TODO(), pgx.LogLevelDebug, "an error has occurred", map[string]interface{}{"test": "value"})
		assert.True(t, strings.Contains(buf.String(), "debug"))
		assert.True(t, strings.Contains(buf.String(), "test: value"))

		buf.Reset()
		tea.SetGlobalLogLevel("info")
		l.Log(context.TODO(), pgx.LogLevelInfo, "an error has occurred", map[string]interface{}{"test": "value"})
		assert.True(t, strings.Contains(buf.String(), "info"))
		assert.True(t, strings.Contains(buf.String(), "test: value"))

		buf.Reset()
		tea.SetGlobalLogLevel("warn")
		l.Log(context.TODO(), pgx.LogLevelWarn, "an error has occurred", map[string]interface{}{"test": "value"})
		assert.True(t, strings.Contains(buf.String(), "warn"))
		assert.True(t, strings.Contains(buf.String(), "test: value"))

		buf.Reset()
		tea.SetGlobalLogLevel("debug")
		l.Log(context.TODO(), pgx.LogLevelError, "an error has occurred", map[string]interface{}{"test": "value"})
		assert.True(t, strings.Contains(buf.String(), "test: value"))
	})

	t.Run("can send goose logs", func(t *testing.T) {
		l := gooseLogger{}
		var buf bytes.Buffer
		tea.SetGlobalLogWriter(&buf)
		defer tea.ResetGlobalLogger()
		tea.SetGlobalLogLevel("info")

		l.Print("an error has occurred")
		assert.True(t, strings.Contains(buf.String(), "an error has occurred"))

		buf.Reset()
		l.Printf("an %s has occurred", "error")
		assert.True(t, strings.Contains(buf.String(), "an error has occurred"))

		buf.Reset()
		l.Println("an error has occurred")
		assert.True(t, strings.Contains(buf.String(), "an error has occurred"))

		buf.Reset()
		l.Fatal("an error has occurred")
		assert.True(t, strings.Contains(buf.String(), "an error has occurred"))

		buf.Reset()
		l.Fatalf("an %s has occurred", "error")
		assert.True(t, strings.Contains(buf.String(), "an error has occurred"))
	})
}

func TestPostgres_Add(t *testing.T) {
	t.Run("can create new instance", func(t *testing.T) {
		client, _, _ := setup(t)
		assert.NotNil(t, client.Add())
	})

	t.Run("can execute", func(t *testing.T) {
		client, _, _ := setup(t)
		add := pgAdd{client: client}
		stmt, _, err := add.
			Item(map[string]interface{}{"test_coverage": 0}).
			Query(client.Query().From("units").Field("testCoverage").First(1)).
			To("tests").
			Statement()
		assert.Nil(t, err)
		assert.Equal(t, "INSERT INTO tests (test_coverage) SELECT test_coverage FROM units LIMIT 1", stmt)
	})
}

func TestPostgres_Query(t *testing.T) {
	t.Run("can create new instance", func(t *testing.T) {
		client, _, _ := setup(t)
		assert.NotNil(t, client.Query())
	})

	t.Run("can execute on primary", func(t *testing.T) {
		client, _, _ := setup(t)
		query := pgQuery{client: client}
		now := time.Now()
		q := query.From("tests").
			Complement("units ON runs.id = units.id").
			Filter(PgCond{}.Gt("coverage", 50)).
			Order("coverage DESC").
			Fields("FOO").
			Transform(strings.ToLower).
			Field("runs").
			First(5).
			After("created_at", &now)

		stmt, args, err := q.Statement()
		assert.Nil(t, err)
		assert.Equal(t, "SELECT runs, foo FROM tests LEFT JOIN units ON runs.id = units.id WHERE coverage > $1 AND created_at > $2 ORDER BY coverage DESC LIMIT 5", stmt)
		assert.Equal(t, []interface{}{50, &now}, args)
		assert.Equal(t, fmt.Sprint(stmt, args), q.String())
	})
}

func TestPostgres_Remove(t *testing.T) {
	t.Run("can create new instance", func(t *testing.T) {
		client, _, _ := setup(t)
		assert.NotNil(t, client.Remove())
	})

	t.Run("can execute", func(t *testing.T) {
		client, _, _ := setup(t)
		remove := pgRemove{client: client}

		now := time.Now()
		stmt, args, err := remove.
			From("tests").
			Filter(PgCond{}.Gt("coverage", 50).Raw("id = tests.id")).
			Order("coverage DESC").
			After("created_at", &now).
			Statement()
		assert.Nil(t, err)
		assert.Equal(t, "DELETE FROM tests WHERE coverage > $1 AND id = tests.id AND created_at > $2 ORDER BY coverage DESC", stmt)
		assert.Equal(t, []interface{}{50, &now}, args)
	})
}

func TestPostgres_Transaction(t *testing.T) {
	t.Run("handles new transaction errors", func(t *testing.T) {
		client, _, secondary := setup(t)
		secondary.Expect("Begin", context.TODO()).
			Return(nil, tea.NewError("an error occurred"))
		defer secondary.Assert(t)

		_, err := client.Transaction(context.TODO(), true)
		assert.NotNil(t, err)
	})

	t.Run("can create new instance", func(t *testing.T) {
		client, primary, _ := setup(t)
		primary.Expect("Begin", context.TODO()).
			Return(NewPostgresTx(t), nil)
		defer primary.Assert(t)

		tx, err := client.Transaction(context.TODO(), false)
		assert.Nil(t, err)
		assert.NotNil(t, tx)
	})

	t.Run("raises bad request errors", func(t *testing.T) {
		ptx := NewPostgresTx(t)
		defer ptx.Assert(t)

		add := mock.NewAdd(t)
		add.Expect("Statement").
			Return("", nil, tea.NewError("an error has occurred"))
		defer add.Assert(t)

		tx := pgTransaction{ctx: context.TODO(), tx: ptx}
		_, err := tx.Execute(add)
		assert.NotNil(t, err)
		assert.False(t, tea.IsFatal(err))
	})

	t.Run("raises non fatal errors on execute", func(t *testing.T) {
		ptx := NewPostgresTx(t)
		ptx.Expect("Exec", context.TODO(), "").
			Return(0, &pgconn.PgError{Code: pgerrcode.IntegrityConstraintViolation})
		defer ptx.Assert(t)

		add := mock.NewAdd(t)
		add.Expect("Statement").
			Return("", nil, nil)
		defer add.Assert(t)

		tx := pgTransaction{ctx: context.TODO(), tx: ptx}
		_, err := tx.Execute(add)
		assert.NotNil(t, err)
		assert.False(t, tea.IsFatal(err))

		ptx.Expect("Query", context.TODO(), "").
			Return(nil, pgx.ErrNoRows)
		defer ptx.Assert(t)

		query := mock.NewQuery(t)
		query.Expect("Statement").
			Return("", nil, nil)
		defer query.Assert(t)

		_, err = tx.Execute(query, nil)
		assert.NotNil(t, err)
	})

	t.Run("raises fatal errors", func(t *testing.T) {
		ptx := NewPostgresTx(t)
		ptx.Expect("Exec", context.TODO(), "").
			Return(0, tea.NewError("an error has occurred"))
		defer ptx.Assert(t)

		add := mock.NewAdd(t)
		add.Expect("Statement").
			Return("", nil, nil)
		defer add.Assert(t)

		tx := pgTransaction{ctx: context.TODO(), tx: ptx}
		_, err := tx.Execute(add)
		assert.NotNil(t, err)
		assert.True(t, tea.IsFatal(err))

		ptx.Expect("Query", context.TODO(), "").
			Return(nil, tea.NewError("an error has occurred"))
		defer ptx.Assert(t)

		query := mock.NewQuery(t)
		query.Expect("Statement").
			Return("", nil, nil)
		defer query.Assert(t)

		_, err = tx.Execute(query, nil)
		assert.NotNil(t, err)
	})

	t.Run("can execute", func(t *testing.T) {
		ptx := NewPostgresTx(t)
		ptx.Expect("Exec", context.TODO(), "").
			Return(pgconn.CommandTag{}, nil)
		defer ptx.Assert(t)

		add := mock.NewAdd(t)
		add.Expect("Statement").
			Return("", nil, nil)
		defer add.Assert(t)

		tx := pgTransaction{ctx: context.TODO(), tx: ptx}
		_, err := tx.Execute(add)
		assert.Nil(t, err)

		rows := NewPostgresRows(t)
		rows.Expect("Close")
		rows.Expect("Next").Return(false)
		rows.Expect("Err").Return(nil)
		rows.Expect("Close")
		ptx.Expect("Query", context.TODO(), "").
			Return(rows, nil)
		defer ptx.Assert(t)

		query := mock.NewQuery(t)
		query.Expect("Statement").
			Return("", nil, nil)
		defer query.Assert(t)

		var dst []map[string]interface{}
		_, err = tx.Execute(query, &dst)
		assert.Nil(t, err)
	})

	t.Run("raises commit errors", func(t *testing.T) {
		ptx := NewPostgresTx(t)
		ptx.Expect("Commit", context.TODO()).
			Return(tea.NewError("an error has occurred"))
		defer ptx.Assert(t)

		tx := pgTransaction{ctx: context.TODO(), tx: ptx}
		err := tx.Commit()
		assert.NotNil(t, err)
		assert.True(t, tea.IsFatal(err))
	})

	t.Run("can commit", func(t *testing.T) {
		ptx := NewPostgresTx(t)
		ptx.Expect("Commit", context.TODO()).
			Return(nil)
		defer ptx.Assert(t)

		tx := pgTransaction{ctx: context.TODO(), tx: ptx}
		err := tx.Commit()
		assert.Nil(t, err)
	})

	t.Run("raises rollback errors", func(t *testing.T) {
		ptx := NewPostgresTx(t)
		ptx.Expect("Rollback", context.TODO()).
			Return(tea.NewError("an error has occurred"))
		defer ptx.Assert(t)

		tx := pgTransaction{ctx: context.TODO(), tx: ptx}
		err := tx.Rollback()
		assert.NotNil(t, err)
		assert.True(t, tea.IsFatal(err))
	})

	t.Run("can rollback", func(t *testing.T) {
		ptx := NewPostgresTx(t)
		ptx.Expect("Rollback", context.TODO()).
			Return(nil)
		defer ptx.Assert(t)

		tx := pgTransaction{ctx: context.TODO(), tx: ptx}
		err := tx.Rollback()
		assert.Nil(t, err)
	})
}

func TestPostgres_Update(t *testing.T) {
	t.Run("can create new instance", func(t *testing.T) {
		client, _, _ := setup(t)
		assert.NotNil(t, client.Update())
	})

	t.Run("can execute", func(t *testing.T) {
		client, _, _ := setup(t)
		update := pgUpdate{client: client}

		stmt, args, err := update.
			In("tests").
			Filter(PgCond{}.Gt("coverage", 50)).
			Item(map[string]interface{}{"coverage": 0}).
			Statement()
		assert.Nil(t, err)
		assert.Equal(t, "UPDATE tests SET coverage = $1 WHERE coverage > $2", stmt)
		assert.Equal(t, []interface{}{0, 50}, args)
	})
}

func TestPostgres_Filter(t *testing.T) {
	t.Run("raises invalid slice type errors", func(t *testing.T) {
		f := PgCond{}.
			Eq("key", []interface{}{}).
			Lt("key", []interface{}{}).
			Gt("key", []interface{}{}).
			NotEq("key", []interface{}{})

		_, _, err := squirrel.Select("column").From("tests").Where(f).ToSql()
		assert.NotNil(t, err)
		assert.False(t, tea.IsFatal(err))
	})

	t.Run("raises bad op errors", func(t *testing.T) {
		f := PgCond{}.
			Lt("key", nil)

		_, _, err := squirrel.Select("column").From("tests").Where(f).ToSql()
		assert.NotNil(t, err)
		assert.False(t, tea.IsFatal(err))
	})

	t.Run("can sql-ize", func(t *testing.T) {
		or := PgCond{}.Lt("lt", 2)
		and := PgCond{}.Gt("gt", 3).
			NotEq("ne", 4).
			BeginsWith("prefix", "5").
			EndsWith("suffix", "6").
			Contains("containsString", "7").
			Contains("containsSlice", []interface{}{8, 9, 10}).
			Contains("containsNumber", 11).
			NotContains("notContainsString", "7").
			NotContains("notContainsSlice", []interface{}{8, 9, 10}).
			NotContains("notContainsNumber", 11)

		f := PgCond{}.
			Eq("eq", 1).
			Or(or).
			And(and)

		stmt, args, err := squirrel.Select("column").From("tests").Where(f).ToSql()
		assert.Nil(t, err)
		assert.Equal(t, "SELECT column FROM tests WHERE eq = ? AND (eq = ? OR lt < ?) AND (eq = ? AND (eq = ? OR lt < ?) AND gt > ? AND ne <> ? AND prefix ILIKE ? AND suffix ILIKE ? AND containsString ILIKE ? AND containsSlice IN (?,?,?) AND containsNumber IN (?) AND notContainsString NOT ILIKE ? AND notContainsSlice NOT IN (?,?,?) AND notContainsNumber NOT IN (?))", stmt)
		assert.Equal(t, []interface{}{1, 1, 2, 1, 1, 2, 3, 4, "5%", "%6", "%7%", 8, 9, 10, 11, "%7%", 8, 9, 10, 11}, args)
	})
}

type PostgresPool struct {
	mock.Mock
	t *testing.T
	pgPool
}

func (p *PostgresPool) Exec(ctx context.Context, sql string, args ...interface{}) (pgconn.CommandTag, error) {
	p.t.Helper()
	res := p.Call(p.t, append([]interface{}{ctx, sql}, args...)...)
	if len(res) != 2 {
		p.Fatalf(p.t, "length of return values for Exec is not equal to 2")
	}

	if res[1] != nil {
		err, ok := res[1].(error)
		if !ok {
			p.Fatalf(p.t, "return value #2 of Exec is not an error")
		}
		return nil, err
	}

	tag, ok := res[0].(pgconn.CommandTag)
	if !ok {
		p.Fatalf(p.t, "return value #1 of Exec is not a pgconn.CommandTag")
	}

	return tag, nil
}

func (p *PostgresPool) Begin(ctx context.Context) (pgx.Tx, error) {
	p.t.Helper()
	res := p.Call(p.t, ctx)
	if len(res) != 2 {
		p.Fatalf(p.t, "length of return values for Begin is not equal to 1")
	}

	if res[1] != nil {
		err, ok := res[1].(error)
		if !ok {
			p.Fatalf(p.t, "return value #2 of Begin is not an error")
		}
		return nil, err
	}

	tx, ok := res[0].(pgx.Tx)
	if !ok {
		p.Fatalf(p.t, "return value #1 of Begin is not a pgx.Tx")
	}

	return tx, nil
}

func NewPostgresPool(t *testing.T) *PostgresPool {
	p := PostgresPool{t: t}

	return &p
}

func setup(t *testing.T) (*pgClient, *PostgresPool, *PostgresPool) {
	t.Helper()

	client := pgClient{
		conf: Config{
			primary:   "postgres://postgres:postgres@db:5432",
			secondary: "postgres://postgres:postgres@db:5432",
		},
		connect: func(ctx context.Context, config *pgxpool.Config) (*pgxpool.Pool, error) {
			return &pgxpool.Pool{}, nil
		},
	}

	assert.Nil(t, client.Connect())
	primary := NewPostgresPool(t)
	secondary := NewPostgresPool(t)
	client.pools.primary = primary
	client.pools.secondary = secondary

	return &client, primary, secondary
}

type PostgresTx struct {
	mock.Mock
	t *testing.T
	pgx.Tx
}

func (tx *PostgresTx) Commit(ctx context.Context) error {
	tx.t.Helper()
	res := tx.Call(tx.t, ctx)
	if len(res) != 1 {
		tx.Fatalf(tx.t, "length of return values for Commit is not equal to 1")
	}

	if res[0] != nil {
		err, ok := res[0].(error)
		if !ok {
			tx.Fatalf(tx.t, "return value #1 of Commit is not an error")
		}
		return err
	}

	return nil
}

func (tx *PostgresTx) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	tx.t.Helper()
	res := tx.Call(tx.t, append([]interface{}{ctx, sql}, args...)...)
	if len(res) != 2 {
		tx.Fatalf(tx.t, "length of return values for Query is not equal to 2")
	}

	if res[1] != nil {
		err, ok := res[1].(error)
		if !ok {
			tx.Fatalf(tx.t, "return value #2 of Query is not an error")
		}
		return nil, err
	}

	rows, ok := res[0].(pgx.Rows)
	if !ok {
		tx.Fatalf(tx.t, "return value #1 of Query is not a pgx.Rows")
	}

	return rows, nil
}

func (tx *PostgresTx) Rollback(ctx context.Context) error {
	tx.t.Helper()
	res := tx.Call(tx.t, ctx)
	if len(res) != 1 {
		tx.Fatalf(tx.t, "length of return values for Rollback is not equal to 1")
	}

	if res[0] != nil {
		err, ok := res[0].(error)
		if !ok {
			tx.Fatalf(tx.t, "return value #1 of Rollback is not an error")
		}
		return err
	}

	return nil
}

func (tx *PostgresTx) Exec(ctx context.Context, sql string, args ...interface{}) (pgconn.CommandTag, error) {
	tx.t.Helper()
	res := tx.Call(tx.t, append([]interface{}{ctx, sql}, args...)...)
	if len(res) != 2 {
		tx.Fatalf(tx.t, "length of return values for Exec is not equal to 2")
	}

	if res[1] != nil {
		err, ok := res[1].(error)
		if !ok {
			tx.Fatalf(tx.t, "return value #2 of Exec is not an error")
		}
		return nil, err
	}

	tag, ok := res[0].(pgconn.CommandTag)
	if !ok {
		tx.Fatalf(tx.t, "return value #2 of Exec is not a pgconn.CommandTag")
	}

	return tag, nil
}

func NewPostgresTx(t *testing.T) *PostgresTx {
	tx := PostgresTx{t: t}

	return &tx
}

type PostgresRows struct {
	mock.Mock
	t *testing.T
	pgx.Rows
}

func (r *PostgresRows) Close() {
	r.t.Helper()
	res := r.Call(r.t)
	if len(res) != 0 {
		r.Fatalf(r.t, "length of return values for Close is not equal to 0")
	}
}

func (r *PostgresRows) Err() error {
	r.t.Helper()
	res := r.Call(r.t)
	if len(res) != 1 {
		r.Fatalf(r.t, "length of return values for Err is not equal to 1")
	}

	if res[0] != nil {
		err, ok := res[0].(error)
		if !ok {
			r.Fatalf(r.t, "return value #1 of Err is not an error")
		}
		return err
	}

	return nil
}

func (r *PostgresRows) Next() bool {
	r.t.Helper()
	res := r.Call(r.t)
	if len(res) != 1 {
		r.Fatalf(r.t, "length of return values for Next is not equal to 1")
	}

	next, ok := res[0].(bool)
	if !ok {
		r.Fatalf(r.t, "return value #1 of Next is not a bool")
	}

	return next
}

func (r *PostgresRows) Scan(dest ...interface{}) error {
	r.t.Helper()
	res := r.Call(r.t, dest...)
	if len(res) != 1 {
		r.Fatalf(r.t, "length of return values for Scan is not equal to 1")
	}

	if res[0] != nil {
		err, ok := res[0].(error)
		if !ok {
			r.Fatalf(r.t, "return value #1 of Scan is not an error")
		}
		return err
	}

	return nil
}

func NewPostgresRows(t *testing.T) *PostgresRows {
	rows := PostgresRows{t: t}

	return &rows
}

type ErrConnector struct{}

func (e ErrConnector) Connect(_ context.Context) (driver.Conn, error) {
	return nil, tea.NewError("an error has occurred")
}

func (e ErrConnector) Driver() driver.Driver {
	panic("not implemented")
}
