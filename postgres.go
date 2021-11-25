package ark

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	_ "github.com/lib/pq"
	"github.com/pghq/go-tea"
	"github.com/pressly/goose/v3"
)

// pgClient is a client for interacting with Postgres.
type pgClient struct {
	pools struct {
		primary   pgPool
		secondary pgPool
	}
	conf    Config
	open    func(driverName, dataSourceName string) (*sql.DB, error)
	connect func(ctx context.Context, config *pgxpool.Config) (*pgxpool.Pool, error)
}

func (c *pgClient) Connect() error {
	primary, err := c.newPool(c.conf.primary)
	if err != nil {
		return tea.Error(err)
	}

	c.pools.primary = primary
	secondary, err := c.newPool(c.conf.secondary)
	if err != nil {
		return tea.Error(err)
	}

	c.pools.secondary = secondary
	if c.conf.migrationFS != nil && c.conf.migrationDirectory != "" {
		db, err := c.open("postgres", c.conf.primary)
		if err != nil {
			return tea.Error(err)
		}
		defer db.Close()
		goose.SetLogger(&gooseLogger{})
		goose.SetBaseFS(c.conf.migrationFS)
		if err := goose.Up(db, c.conf.migrationDirectory); err != nil {
			_ = goose.Down(db, c.conf.migrationDirectory)
			return tea.Error(err)
		}
	}

	return nil
}

// newPool creates a new concurrency safe pool
func (c *pgClient) newPool(databaseURL string) (pgPool, error) {
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, tea.Error(err)
	}

	config.ConnConfig.Logger = &pgxLogger{}
	config.MaxConnLifetime = c.conf.maxConnLifetime
	config.MaxConns = int32(c.conf.maxConns)

	pool, err := c.connect(context.Background(), config)
	if err != nil {
		return nil, tea.Error(err)
	}

	return pool, nil
}

// pgPool for executing db commands against.
type pgPool interface {
	Exec(ctx context.Context, sql string, args ...interface{}) (pgconn.CommandTag, error)
	Begin(ctx context.Context) (pgx.Tx, error)
}

// pgxLogger is an instance of the pgx Logger
type pgxLogger struct{}

func (l *pgxLogger) Log(_ context.Context, level pgx.LogLevel, msg string, data map[string]interface{}) {
	switch level {
	case pgx.LogLevelInfo:
		tea.Logf("info", "%s\n%s", msg, l.pretty(data))
	case pgx.LogLevelWarn:
		tea.Logf("warn", "%s\n%s", msg, l.pretty(data))
	default:
		tea.Logf("debug", "%s\n%s", msg, l.pretty(data))
	}
}

func (l *pgxLogger) pretty(data map[string]interface{}) string {
	var spaces int
	for k, _ := range data {
		if len(k) > spaces {
			spaces = len(k)
		}
	}

	var sb strings.Builder
	for k, v := range data {
		sb.WriteString(fmt.Sprintf("%*s: %+v\n", spaces, k, v))
	}

	return strings.TrimSuffix(sb.String(), "\n")
}

// gooseLogger is a custom logger for the goose package
type gooseLogger struct{}

func (g *gooseLogger) Fatal(v ...interface{}) {
	tea.SendError(tea.NewError(v...))
}

func (g *gooseLogger) Fatalf(format string, v ...interface{}) {
	tea.SendError(tea.NewErrorf(format, v...))
}

func (g *gooseLogger) Print(v ...interface{}) {
	tea.Log("info", v...)
}

func (g *gooseLogger) Println(v ...interface{}) {
	tea.Log("info", v...)
}

func (g *gooseLogger) Printf(format string, v ...interface{}) {
	tea.Logf("info", format, v...)
}

// IsPgIntegrityConstraintViolation checks if the error is a pg integrity constraint violation.
func IsPgIntegrityConstraintViolation(err error) bool {
	var e *pgconn.PgError
	if tea.AsError(err, &e) && pgerrcode.IsIntegrityConstraintViolation(e.Code) {
		return true
	}

	return false
}
