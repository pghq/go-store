package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	_ "github.com/lib/pq"
	"github.com/pghq/go-museum/museum/diagnostic/errors"
	"github.com/pghq/go-museum/museum/diagnostic/log"
	"github.com/pressly/goose/v3"

	"github.com/pghq/go-datastore/datastore"
)

const (
	// DefaultSQLMaxOpenConns is the default maximum number of open connections.
	DefaultSQLMaxOpenConns = 100
)

// Store is a client for interacting with Postgres.
type Store struct {
	pool            Pool
	secondary       Pool
	primaryDSN      string
	secondaryDSN    string
	maxConns        int
	maxConnLifetime time.Duration
	migrations      struct {
		open      func(driverName, dataSourceName string) (*sql.DB, error)
		fs        fs.FS
		directory string
	}
	connect func(ctx context.Context, config *pgxpool.Config) (*pgxpool.Pool, error)
}

// MaxConns sets the max number of open connections.
func (s *Store) MaxConns(conns int) *Store {
	s.maxConns = conns

	return s
}

// MaxConnLifetime sets the max lifetime for a connection.
func (s *Store) MaxConnLifetime(timeout time.Duration) *Store {
	s.maxConnLifetime = timeout

	return s
}

func (s *Store) Secondary(dsn string) *Store {
	if dsn != "" {
		s.secondaryDSN = dsn
	}

	return s
}

// Migrations sets the database migration configuration
func (s *Store) Migrations(fs fs.FS, directory string) *Store {
	s.migrations.fs = fs
	s.migrations.directory = directory

	return s
}

func (s *Store) Connect() error {
	primary, err := s.newPool(s.primaryDSN)
	if err != nil {
		return errors.Wrap(err)
	}

	s.pool = primary
	secondary, err := s.newPool(s.secondaryDSN)
	if err != nil {
		return errors.Wrap(err)
	}

	s.secondary = secondary

	if s.migrations.fs != nil && s.migrations.directory != "" {
		db, err := s.migrations.open("postgres", s.primaryDSN)
		if err != nil {
			return errors.Wrap(err)
		}
		defer db.Close()
		goose.SetLogger(NewGooseLogger())
		goose.SetBaseFS(s.migrations.fs)
		if err := goose.Up(db, s.migrations.directory); err != nil {
			_ = goose.Down(db, s.migrations.directory)
			return errors.Wrap(err)
		}
	}

	return nil
}

// newPool creates a new concurrency safe pool
func (s *Store) newPool(databaseURL string) (Pool, error) {
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, errors.Wrap(err)
	}

	config.ConnConfig.Logger = NewPGXLogger()
	config.MaxConnLifetime = s.maxConnLifetime
	config.MaxConns = int32(s.maxConns)

	pool, err := s.connect(context.Background(), config)
	if err != nil {
		return nil, errors.Wrap(err)
	}

	return pool, nil
}

// New creates a new Postgres database client.
func New(primary string) *Store {
	s := Store{
		primaryDSN:   primary,
		secondaryDSN: primary,
		connect:      pgxpool.ConnectConfig,
	}
	s.maxConns = DefaultSQLMaxOpenConns
	s.migrations.open = sql.Open

	return &s
}

// Pool for executing db commands against.
type Pool interface {
	Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error)
	Exec(ctx context.Context, sql string, args ...interface{}) (pgconn.CommandTag, error)
	Begin(ctx context.Context) (pgx.Tx, error)
}

// Cursor represents an instance of a Cursor
type Cursor struct {
	dest []interface{}
	rows pgx.Rows
}

func (c *Cursor) Next() bool {
	return c.rows.Next()
}

func (c *Cursor) Decode(values ...interface{}) error {
	if err := c.rows.Scan(values...); err != nil {
		return errors.Wrap(err)
	}

	return nil
}

func (c *Cursor) Close() {
	c.rows.Close()
}

func (c *Cursor) Error() error {
	if err := c.rows.Err(); err != nil {
		return errors.Wrap(err)
	}

	return nil
}

// NewCursor constructs a new cursor instance.
func NewCursor(rows pgx.Rows) datastore.Cursor {
	return &Cursor{
		rows: rows,
	}
}

// PGXLogger is an instance of the pgx Logger
type PGXLogger struct{}

func (l *PGXLogger) Log(_ context.Context, level pgx.LogLevel, msg string, _ map[string]interface{}) {
	switch level {
	case pgx.LogLevelDebug:
		log.Debug(msg)
	case pgx.LogLevelInfo:
		log.Info(msg)
	case pgx.LogLevelWarn:
		log.Warn(msg)
	default:
		errors.Send(errors.New(msg))
	}
}

// NewPGXLogger creates a new database pgx Logger
func NewPGXLogger() *PGXLogger {
	return &PGXLogger{}
}

// GooseLogger is a custom logger for the goose package
type GooseLogger struct{}

func (g *GooseLogger) Fatal(v ...interface{}) {
	errors.Send(errors.New(v...))
}

func (g *GooseLogger) Fatalf(format string, v ...interface{}) {
	errors.Send(errors.Newf(format, v...))
}

func (g *GooseLogger) Print(v ...interface{}) {
	log.Info(fmt.Sprint(v...))
}

func (g *GooseLogger) Println(v ...interface{}) {
	log.Info(fmt.Sprintln(v...))
}

func (g *GooseLogger) Printf(format string, v ...interface{}) {
	log.Infof(format, v...)
}

// NewGooseLogger creates a new goose logger
func NewGooseLogger() *GooseLogger {
	return &GooseLogger{}
}

// IsIntegrityConstraintViolation checks if the error is an integrity constraint violation.
func IsIntegrityConstraintViolation(err error) bool {
	var e *pgconn.PgError
	if errors.As(err, &e) && pgerrcode.IsIntegrityConstraintViolation(e.Code) {
		return true
	}

	return false
}
