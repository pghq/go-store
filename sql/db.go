package sql

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/luna-duclos/instrumentedsql"
	"github.com/pghq/go-tea"
	"github.com/pressly/goose/v3"

	"github.com/pghq/go-ark/db"
)

// DB SQL database
type DB struct {
	backend *sqlx.DB
	err     error
}

func (d DB) Ping(ctx context.Context) error {
	if d.err != nil {
		return tea.Error(d.err)
	}

	return d.backend.PingContext(ctx)
}

// NewDB Create a new SQL database
func NewDB(opts ...db.Option) *DB {
	config := db.ConfigWith(opts)
	d := DB{}

	if config.SQLTraceDriver != nil {
		trace(config.DriverName, config.SQLTraceDriver)
		config.SQLOpenFunc = func(driverName, dataSourceName string) (*sql.DB, error) {
			return sql.Open(fmt.Sprintf("!ark!%s", driverName), dataSourceName)
		}
	}

	if config.SQL == nil {
		sdb, err := config.SQLOpenFunc(config.DriverName, config.DSN)
		if err != nil {
			d.err = err
			return &d
		}
		config.SQL = sdb
	}

	if config.MigrationFS != nil && config.MigrationDirectory != "" {
		goose.SetLogger(gooseLogger{})
		goose.SetBaseFS(config.MigrationFS)
		if err := goose.Up(config.SQL, config.MigrationDirectory); err != nil {
			_ = goose.Down(config.SQL, config.MigrationDirectory)
			d.err = err
			return &d
		}
	}

	d.backend = sqlx.NewDb(config.SQL, config.DriverName)
	d.backend.SetConnMaxLifetime(config.MaxConnLifetime)
	d.backend.SetConnMaxIdleTime(config.MaxIdleLifetime)
	d.backend.SetMaxOpenConns(config.MaxConns)

	return &d
}

// gooseLogger Custom goose logger implementation
type gooseLogger struct{}

func (g gooseLogger) Fatal(v ...interface{}) {
	tea.SendError(tea.NewError(v...))
}

func (g gooseLogger) Fatalf(format string, v ...interface{}) {
	tea.SendError(tea.NewErrorf(format, v...))
}

func (g gooseLogger) Print(v ...interface{}) {
	tea.Log("info", v...)
}

func (g gooseLogger) Println(v ...interface{}) {
	tea.Log("info", v...)
}

func (g gooseLogger) Printf(format string, v ...interface{}) {
	tea.Logf("info", format, v...)
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

// trace register a new logging hook for the driver
func trace(driverName string, driver driver.Driver) {
	drivers := sql.Drivers()
	present := false
	name := fmt.Sprintf("!ark!%s", driverName)
	for _, d := range drivers {
		if d == name {
			present = true
		}
	}

	logger := instrumentedsql.LoggerFunc(func(ctx context.Context, msg string, args ...interface{}) {
		tea.Logf("info", "%s: %+v", msg, args)
	})

	if !present {
		sql.Register(name, instrumentedsql.WrapDriver(driver, instrumentedsql.WithLogger(logger)))
	}
}
