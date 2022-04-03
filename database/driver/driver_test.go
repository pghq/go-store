package driver

import (
	"fmt"
	"os"
	"testing"

	"github.com/pghq/go-tea/trail"
)

func TestMain(m *testing.M) {
	trail.Testing()
	var teardown func()
	env := os.Getenv("APP_ENV")
	os.Setenv("APP_ENV", "dev")
	defer os.Setenv("APP_ENV", env)

	postgres, teardown = NewTestPostgresDB()
	defer teardown()

	os.Exit(m.Run())
}

// must be nil error or panic
func must(err error) {
	if err != nil {
		trail.OneOff(fmt.Sprintf("%+v", err))
		os.Exit(1)
	}
}
