package provider

import (
	"context"
)

type uowContextKey = struct{}

func WithUnitOfWork(ctx context.Context, uow UnitOfWork) context.Context {
	return context.WithValue(ctx, uowContextKey{}, uow)
}

func UnitOfWorkValue(ctx context.Context) (UnitOfWork, bool) {
	uow, ok := ctx.Value(uowContextKey{}).(UnitOfWork)
	return uow, ok
}
