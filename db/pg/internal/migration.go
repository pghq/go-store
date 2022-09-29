package internal

import (
	"database/sql"
	"fmt"
	"io/fs"

	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/pghq/go-tea/trail"
	"github.com/pressly/goose/v3"
)

// Apply migration
func Apply(db *sql.DB, fs fs.FS) error {
	if fs != nil {
		goose.SetLogger(gooseLogger{})
		goose.SetBaseFS(fs)
		goose.SetTableName("migrations")
		_ = goose.SetDialect("pgx")

		if err := goose.Up(db, "migrations"); err != nil {
			_ = goose.Down(db, "migrations")
			return trail.Stacktrace(err)
		}
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
