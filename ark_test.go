package ark

import (
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/pghq/go-tea/trail"
	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	trail.Testing()
	t.Parallel()

	t.Run("bad dsn", func(t *testing.T) {
		// https://stackoverflow.com/questions/48671938/go-url-parsestring-fails-with-certain-user-names-or-passwords
		_, err := New(WithDSN("sqlite3", "sql://user:abc{DEf1=ghi@example.com:5432/db?sslmode=require"))
		assert.NotNil(t, err)
	})

	t.Run("ok", func(t *testing.T) {
		store, _ := New(WithDSN("sqlite3", ":memory:"))
		assert.NotNil(t, store)
	})
}
