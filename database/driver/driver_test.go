package driver

import (
	"github.com/pghq/go-tea"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	tea.Testing()
	var teardown func()

	postgres, teardown = NewTestPostgresDB()
	defer teardown()

	os.Exit(m.Run())
}

// must be nil error or panic
func must(err error) {
	if err != nil {
		panic(err)
	}
}
