package driver

import (
	"os"
	"testing"

	"github.com/pghq/go-tea/trail"
)

func TestMain(m *testing.M) {
	trail.Testing()
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
