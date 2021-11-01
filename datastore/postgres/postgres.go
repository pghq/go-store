// Copyright 2021 PGHQ. All Rights Reserved.
//
// Licensed under the GNU General Public License, Version 3 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package postgres provides a store implementation using Postgres.
package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"strings"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	_ "github.com/lib/pq"
	"github.com/pghq/go-museum/museum/diagnostic/errors"
	"github.com/pghq/go-museum/museum/diagnostic/log"
	"github.com/pressly/goose/v3"
)

const (
	// DefaultSQLMaxOpenConns is the default maximum number of open connections.
	DefaultSQLMaxOpenConns = 100
)

// Client is a client for interacting with Postgres.
type Client struct {
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
func (c *Client) MaxConns(conns int) *Client {
	c.maxConns = conns

	return c
}

// MaxConnLifetime sets the max lifetime for a connection.
func (c *Client) MaxConnLifetime(timeout time.Duration) *Client {
	c.maxConnLifetime = timeout

	return c
}

func (c *Client) Secondary(dsn string) *Client {
	if dsn != "" {
		c.secondaryDSN = dsn
	}

	return c
}

// Migrations sets the database migration configuration
func (c *Client) Migrations(fs fs.FS, directory string) *Client {
	c.migrations.fs = fs
	c.migrations.directory = directory

	return c
}

func (c *Client) Connect() error {
	primary, err := c.newPool(c.primaryDSN)
	if err != nil {
		return errors.Wrap(err)
	}

	c.pool = primary
	secondary, err := c.newPool(c.secondaryDSN)
	if err != nil {
		return errors.Wrap(err)
	}

	c.secondary = secondary

	if c.migrations.fs != nil && c.migrations.directory != "" {
		db, err := c.migrations.open("postgres", c.primaryDSN)
		if err != nil {
			return errors.Wrap(err)
		}
		defer db.Close()
		goose.SetLogger(NewGooseLogger())
		goose.SetBaseFS(c.migrations.fs)
		if err := goose.Up(db, c.migrations.directory); err != nil {
			_ = goose.Down(db, c.migrations.directory)
			return errors.Wrap(err)
		}
	}

	return nil
}

// newPool creates a new concurrency safe pool
func (c *Client) newPool(databaseURL string) (Pool, error) {
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, errors.Wrap(err)
	}

	config.ConnConfig.Logger = NewPGXLogger()
	config.MaxConnLifetime = c.maxConnLifetime
	config.MaxConns = int32(c.maxConns)

	pool, err := c.connect(context.Background(), config)
	if err != nil {
		return nil, errors.Wrap(err)
	}

	return pool, nil
}

// New creates a new Postgres database client.
func New(primary string) *Client {
	c := Client{
		primaryDSN:   primary,
		secondaryDSN: primary,
		connect:      pgxpool.ConnectConfig,
	}
	c.maxConns = DefaultSQLMaxOpenConns
	c.migrations.open = sql.Open

	return &c
}

// Pool for executing db commands against.
type Pool interface {
	Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error)
	Exec(ctx context.Context, sql string, args ...interface{}) (pgconn.CommandTag, error)
	Begin(ctx context.Context) (pgx.Tx, error)
}

// PGXLogger is an instance of the pgx Logger
type PGXLogger struct{}

func (l *PGXLogger) Log(_ context.Context, level pgx.LogLevel, msg string, data map[string]interface{}) {
	switch level {
	case pgx.LogLevelInfo:
		log.Infof("%s\n%s", msg, l.pretty(data))
	case pgx.LogLevelWarn:
		log.Warnf("%s\n%s", msg, l.pretty(data))
	default:
		log.Debugf("%s\n%s", msg, l.pretty(data))
	}
}

func (l *PGXLogger) pretty(data map[string]interface{}) string {
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
	log.Info(v...)
}

func (g *GooseLogger) Println(v ...interface{}) {
	log.Info(v...)
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
