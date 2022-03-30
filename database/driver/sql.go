package driver

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"

	"github.com/pghq/go-tea"
	"github.com/pressly/goose/v3"

	"github.com/pghq/go-ark/database"
)

// SQL database
type SQL struct {
	backend db
}

func (d SQL) Ping(ctx context.Context) error {
	return d.backend.Ping(ctx)
}

// NewSQL Create a new SQL database
func NewSQL(dialect string, databaseURL *url.URL, opts ...database.Option) (*SQL, error) {
	config := database.ConfigWith(opts)
	db := SQL{}
	var err error
	switch dialect {
	case "postgres", "redshift":
		db.backend, err = newPostgres(dialect, databaseURL, config)
	default:
		return nil, tea.Err("unrecognized dialect")
	}

	if err != nil {
		return nil, tea.Stacktrace(err)
	}

	if config.MigrationFS != nil && config.MigrationDirectory != "" {
		goose.SetLogger(gooseLogger{})
		goose.SetBaseFS(config.MigrationFS)
		err := goose.SetDialect(dialect)
		if err == nil {
			goose.SetTableName(config.MigrationTable)
			err = goose.Up(db.backend.SQL(), config.MigrationDirectory)
		}

		if err != nil {
			_ = goose.Down(db.backend.SQL(), config.MigrationDirectory)
			return nil, tea.Stacktrace(err)
		}
	}

	return &db, nil
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

type db interface {
	Ping(ctx context.Context) error
	Txn(ctx context.Context, opts *sql.TxOptions) (uow, error)
	SQL() *sql.DB
	URL() *url.URL
	placeholder() placeholder
}
