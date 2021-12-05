package internal

import (
	"testing"

	"github.com/pghq/go-tea"
	"github.com/stretchr/testify/assert"
)

func TestResolve(t *testing.T) {
	t.Run("exec response", func(t *testing.T) {
		anError := tea.NewError("an error")
		delta, err := ExecResponse(1, anError).Resolve()
		assert.Equal(t, anError, err)
		assert.Equal(t, 1, delta)
	})
}
