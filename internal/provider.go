package internal

import (
	"context"
)

// Provider is a low-level provider for databases.
type Provider interface {
	Connect(ctx context.Context) error
	Txn(ctx context.Context, ro ...bool) (Txn, error)
}

// Txn represents a database transaction.
type Txn interface {
	Exec(statement Stmt, dst ...interface{}) Resolver
	Commit() error
	Rollback() error
}

// Resolver represents a database execution result
type Resolver interface {
	Resolve() (int, error)
}

// ResolverFunc is a func resolver
type ResolverFunc func() (int, error)

func (r ResolverFunc) Resolve() (int, error) {
	return r()
}

// ExecResponse creates a new resolver for txn.Exec
func ExecResponse(delta int, err error) Resolver {
	return ResolverFunc(func() (int, error) {
		return delta, err
	})
}
