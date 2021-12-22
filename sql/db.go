package sql

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/pghq/go-tea"
	"github.com/pressly/goose/v3"

	"github.com/pghq/go-ark/database"
)

// DB SQL database
type DB struct {
	backend *sqlx.DB
	err     error
	ph      placeholder
}

func (d DB) Ping(ctx context.Context) error {
	if d.err != nil {
		return tea.Stack(d.err)
	}

	return d.backend.PingContext(ctx)
}

// NewDB Create a new SQL database
func NewDB(driverName string, databaseURL *url.URL, opts ...database.Option) *DB {
	config := database.ConfigWith(opts)
	d := DB{}
	sdb, err := config.SQLOpenFunc(driverName, databaseURL.String())
	if err != nil {
		d.err = tea.Stack(err)
		return &d
	}

	if config.PlaceholderPrefix == "" {
		config.PlaceholderPrefix = "$"
		if driverName != "postgres" && driverName != "redshift" {
			config.PlaceholderPrefix = "?"
		}
	}

	if config.MigrationFS != nil && config.MigrationDirectory != "" {
		goose.SetLogger(gooseLogger{})
		goose.SetBaseFS(config.MigrationFS)
		err := goose.SetDialect(driverName)
		if err == nil {
			goose.SetTableName(config.MigrationTable)
			err = goose.Up(sdb, config.MigrationDirectory)
		}

		if err != nil {
			_ = goose.Down(sdb, config.MigrationDirectory)
			d.err = tea.Stack(err)
			return &d
		}
	}

	d.backend = sqlx.NewDb(sdb, driverName)
	d.backend.SetConnMaxLifetime(config.MaxConnLifetime)
	d.backend.SetConnMaxIdleTime(config.MaxIdleLifetime)
	d.backend.SetMaxOpenConns(config.MaxConns)
	d.ph = placeholder(config.PlaceholderPrefix)
	return &d
}

// gooseLogger Custom goose logger implementation
type gooseLogger struct{}

func (g gooseLogger) Fatal(v ...interface{}) {
	tea.Log(context.Background(), "error", tea.Err(v...))
}

func (g gooseLogger) Fatalf(format string, v ...interface{}) {
	tea.Log(context.Background(), "error", tea.Errf(format, v...))
}

func (g gooseLogger) Print(v ...interface{}) {
	tea.Log(context.Background(), "info", v...)
}

func (g gooseLogger) Println(v ...interface{}) {
	tea.Log(context.Background(), "info", v...)
}

func (g gooseLogger) Printf(format string, v ...interface{}) {
	tea.Logf(context.Background(), "info", format, v...)
}

// placeholder placeholder prefix for replacing ?s
type placeholder string

func (ph placeholder) ReplacePlaceholders(sql string) (string, error) {
	if ph == "" || ph == "?" {
		return sql, nil
	}

	buf := &bytes.Buffer{}
	i := 0
	for {
		p := strings.Index(sql, "?")
		if p == -1 {
			break
		}

		if len(sql[p:]) > 1 && sql[p:p+2] == "??" {
			buf.WriteString(sql[:p])
			buf.WriteString("?")
			sql = sql[p+2:]
		} else {
			i++
			buf.WriteString(sql[:p])
			_, _ = fmt.Fprintf(buf, "%s%d", ph, i)
			sql = sql[p+1:]
		}
	}

	buf.WriteString(sql)
	return buf.String(), nil
}
