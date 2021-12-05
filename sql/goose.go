package sql

import "github.com/pghq/go-tea"

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
