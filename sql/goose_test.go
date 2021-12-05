package sql

import (
	"bytes"
	"strings"
	"testing"

	"github.com/pghq/go-tea"
	"github.com/stretchr/testify/assert"
)

func TestGooseLogger_Print(t *testing.T) {
	l := gooseLogger{}
	var buf bytes.Buffer
	tea.SetGlobalLogWriter(&buf)
	defer tea.ResetGlobalLogger()
	tea.SetGlobalLogLevel("info")

	t.Run("logs message", func(t *testing.T) {
		l.Print("an error has occurred")
		assert.True(t, strings.Contains(buf.String(), "an error has occurred"))
	})
}

func TestGooseLogger_Printf(t *testing.T) {
	l := gooseLogger{}
	var buf bytes.Buffer
	tea.SetGlobalLogWriter(&buf)
	defer tea.ResetGlobalLogger()
	tea.SetGlobalLogLevel("info")

	t.Run("logs message", func(t *testing.T) {
		l.Printf("an %s has occurred", "error")
		assert.True(t, strings.Contains(buf.String(), "an error has occurred"))
	})
}

func TestGooseLogger_Println(t *testing.T) {
	l := gooseLogger{}
	var buf bytes.Buffer
	tea.SetGlobalLogWriter(&buf)
	defer tea.ResetGlobalLogger()
	tea.SetGlobalLogLevel("info")

	t.Run("logs message", func(t *testing.T) {
		l.Println("an error has occurred")
		assert.True(t, strings.Contains(buf.String(), "an error has occurred"))
	})
}

func TestGooseLogger_Fatal(t *testing.T) {
	l := gooseLogger{}
	var buf bytes.Buffer
	tea.SetGlobalLogWriter(&buf)
	defer tea.ResetGlobalLogger()
	tea.SetGlobalLogLevel("info")

	t.Run("logs message", func(t *testing.T) {
		l.Fatal("an error has occurred")
		assert.True(t, strings.Contains(buf.String(), "an error has occurred"))
	})
}

func TestGooseLogger_Fatalf(t *testing.T) {
	l := gooseLogger{}
	var buf bytes.Buffer
	tea.SetGlobalLogWriter(&buf)
	defer tea.ResetGlobalLogger()
	tea.SetGlobalLogLevel("info")

	t.Run("logs message", func(t *testing.T) {
		l.Fatalf("an %s has occurred", "error")
		assert.True(t, strings.Contains(buf.String(), "an error has occurred"))
	})
}
