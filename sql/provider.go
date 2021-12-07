package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io/fs"
	"strings"
	"time"

	"github.com/Masterminds/squirrel"
	"github.com/jmoiron/sqlx"
	"github.com/luna-duclos/instrumentedsql"
	"github.com/luna-duclos/instrumentedsql/opentracing"
	"github.com/pghq/go-tea"
	"github.com/pressly/goose/v3"

	"github.com/pghq/go-ark/internal"
)

const (
	// DefaultMaxConns is the default maximum number of open connections.
	DefaultMaxConns = 100

	// DefaultMaxIdleLifetime is the default maximum number of open connections.
	DefaultMaxIdleLifetime = 5 * time.Minute
)

// Provider is a low-level provider for the SQL databases.
type Provider struct {
	driver string
	dsn    string
	conf   Config
	client *sqlx.DB
	open   func(driver, dsn string) (*sql.DB, error)
}

func (p *Provider) Connect(_ context.Context) error {
	if p.conf.TraceDriver != nil {
		trace(p.driver, p.conf.TraceDriver)
	}

	client, err := p.connect()
	if err != nil {
		return tea.Error(err)
	}

	p.client = client
	if p.conf.MigrationFS != nil && p.conf.MigrationDirectory != "" {
		goose.SetLogger(&gooseLogger{})
		goose.SetBaseFS(p.conf.MigrationFS)
		if err := goose.Up(client.DB, p.conf.MigrationDirectory); err != nil {
			_ = goose.Down(client.DB, p.conf.MigrationDirectory)
			return tea.Error(err)
		}
	}

	return nil
}

// NewProvider creates a new SQL provider
func NewProvider(driver string, source interface{}, conf Config) *Provider {
	dsn, _ := source.(string)
	return &Provider{
		driver: driver,
		dsn:    dsn,
		conf:   conf,
		open: func(driver, dsn string) (*sql.DB, error) {
			if conf.TraceDriver != nil {
				driver = fmt.Sprintf("ark:%s", driver)
			}
			return sql.Open(driver, dsn)
		},
	}
}

// Config is a configuration for the provider
type Config struct {
	TraceDriver          driver.Driver
	DB                   *sql.DB
	MaxConns             int
	MaxConnLifetime      time.Duration
	MaxIdleLifetime      time.Duration
	MigrationFS          fs.FS
	MigrationDirectory   string
	SQLPlaceholderPrefix internal.SQLPlaceholderPrefix
}

// Filter is an instance of filter conditions for pg queries
type Filter []squirrel.Sqlizer

// Eq is the = operator
func (f Filter) Eq(key string, value interface{}) Filter {
	return append(f, squirrel.Eq{key: value})
}

// Lt is the < operator
func (f Filter) Lt(key string, value interface{}) Filter {
	return append(f, squirrel.Lt{key: value})
}

// Gt is the > operator
func (f Filter) Gt(key string, value interface{}) Filter {
	return append(f, squirrel.Gt{key: value})
}

// NotEq is the <> operator
func (f Filter) NotEq(key string, value interface{}) Filter {
	return append(f, squirrel.NotEq{key: value})
}

// BeginsWith is the LIKE 'foo%' operation
func (f Filter) BeginsWith(key string, value string) Filter {
	return append(f, squirrel.ILike{key: fmt.Sprintf("%s%%", value)})
}

// EndsWith is the LIKE '%foo' operation
func (f Filter) EndsWith(key string, value string) Filter {
	return append(f, squirrel.ILike{key: fmt.Sprintf("%%%s", value)})
}

// Contains is the LIKE '%foo%' operation for strings or IN operator for arrays
func (f Filter) Contains(key string, value interface{}) Filter {
	if _, ok := value.(string); ok {
		return append(f, squirrel.ILike{key: fmt.Sprintf("%%%s%%", value)})
	}

	if _, ok := value.([]interface{}); ok {
		return append(f, squirrel.Eq{key: value})
	}

	return append(f, squirrel.Eq{key: []interface{}{value}})
}

// NotContains is the NOT LIKE '%foo%' operation for strings or NOT IN operator for arrays
func (f Filter) NotContains(key string, value interface{}) Filter {
	if _, ok := value.(string); ok {
		return append(f, squirrel.NotILike{key: fmt.Sprintf("%%%s%%", value)})
	}

	if _, ok := value.([]interface{}); ok {
		return append(f, squirrel.NotEq{key: value})
	}

	return append(f, squirrel.NotEq{key: []interface{}{value}})
}

// Or conjunction
func (f Filter) Or(another Filter) Filter {
	return append(f, squirrel.Or{f, another})
}

// And conjunction
func (f Filter) And(another Filter) Filter {
	return append(f, squirrel.And{f, another})
}

// Expr constructs a raw sql expression
func (f Filter) Expr(sql string, args ...interface{}) Filter {
	return append(f, squirrel.Expr(sql, args...))
}

func (f Filter) ToSql() (string, []interface{}, error) {
	var statements []string
	var arguments []interface{}

	for _, opt := range f {
		s, args, err := opt.ToSql()
		if err != nil {
			return "", nil, tea.BadRequest(err)
		}
		statements = append(statements, s)
		arguments = append(arguments, args...)
	}

	return strings.Join(statements, " AND "), arguments, nil
}

// Ft is a filter object for SQL backends
func Ft() Filter {
	return Filter{}
}

// trace database requests
func trace(driverName string, driver driver.Driver) {
	drivers := sql.Drivers()
	present := false
	name := fmt.Sprintf("ark:%s", driverName)
	for _, d := range drivers {
		if d == name {
			present = true
		}
	}

	if present {
		return
	}

	logger := instrumentedsql.LoggerFunc(func(ctx context.Context, msg string, args ...interface{}) {
		tea.Logf("info", "%s %v", msg, args)
	})

	driverOpts := []instrumentedsql.Opt{
		instrumentedsql.WithLogger(logger),
		instrumentedsql.WithTracer(opentracing.NewTracer(false)),
	}

	sql.Register(name, instrumentedsql.WrapDriver(driver, driverOpts...))
}

// connect creates a new concurrency safe SQLx DB
func (p *Provider) connect() (*sqlx.DB, error) {
	conf := p.conf
	if conf.DB == nil {
		db, err := p.open(p.driver, p.dsn)
		if err != nil {
			return nil, tea.Error(err)
		}
		conf.DB = db
	}

	client := sqlx.NewDb(conf.DB, p.driver)
	maxConns := conf.MaxConns
	if maxConns == 0 {
		maxConns = DefaultMaxConns
	}

	maxIdleLifetime := conf.MaxIdleLifetime
	if maxIdleLifetime == 0 {
		maxIdleLifetime = DefaultMaxIdleLifetime
	}

	client.SetConnMaxLifetime(conf.MaxConnLifetime)
	client.SetConnMaxIdleTime(maxIdleLifetime)
	client.SetMaxOpenConns(maxConns)

	if err := client.Ping(); err != nil {
		client.Close()
		return nil, err
	}

	return client, nil
}
