package internal

import (
	"database/sql"
	"fmt"
	"io/fs"

	"github.com/pghq/go-tea/trail"
	"github.com/pressly/goose/v3"
)

// Config is a configuration for migrations.
type Config struct {
	DB      *sql.DB
	FS      fs.FS
	Dialect string
}

// Apply migration
func Apply(conf Config) error {
	goose.SetLogger(gooseLogger{})
	goose.SetBaseFS(conf.FS)
	_ = goose.SetDialect(conf.Dialect)

	if err := goose.Up(conf.DB, "migrations"); err != nil {
		_ = goose.Down(conf.DB, "migrations")
		return trail.Stacktrace(err)
	}

	return nil
}

// gooseLogger Custom goose logger implementation
type gooseLogger struct{}

func (g gooseLogger) Fatal(v ...interface{}) {
	trail.Fatal(fmt.Sprint(v...))
}

func (g gooseLogger) Fatalf(format string, v ...interface{}) {
	trail.Fatalf(format, v...)
}

func (g gooseLogger) Print(v ...interface{}) {
	trail.Info(fmt.Sprint(v...))
}

func (g gooseLogger) Println(v ...interface{}) {
	trail.Info(fmt.Sprint(v...))
}

func (g gooseLogger) Printf(format string, v ...interface{}) {
	trail.Infof(format, v...)
}
