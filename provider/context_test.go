package provider

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWithUnitOfWork(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		ctx := WithUnitOfWork(context.TODO(), nil)
		uow, _ := UnitOfWorkValue(ctx)
		assert.Nil(t, uow)
	})
}
