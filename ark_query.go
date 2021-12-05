package ark

import (
	"time"

	"github.com/pghq/go-ark/internal"
)

// Qy creates a Query instance
func Qy() Query {
	return Query{}
}

// Query the database
type Query struct {
	l internal.List
}

// Table sets the primary query table
func (q Query) Table(table string) Query {
	q.l.Table = table
	return q
}

// LeftJoin another table
func (q Query) LeftJoin(join string, args ...interface{}) Query {
	q.l.LeftJoins = append(q.l.LeftJoins, internal.LeftJoin{
		Join: join,
		Args: args,
	})
	return q
}

// Fields sets the fields to be returned
func (q Query) Fields(fields ...interface{}) Query {
	q.l.Fields = append(q.l.Fields, internal.Fields(fields...))
	return q
}

// FieldFunc sets a field mapping func
func (q Query) FieldFunc(fn func(string) string) Query {
	q.l.FieldFunc = fn
	return q
}

// Filter by criteria
func (q Query) Filter(filter interface{}) Query {
	q.l.Filter = filter
	return q
}

// OrderBy clause
func (q Query) OrderBy(orderBy string) Query {
	q.l.OrderBy = append(q.l.OrderBy, orderBy)
	return q
}

// First limits the results
func (q Query) First(first int) Query {
	q.l.Limit = first
	return q
}

// After sets the cursor starting point
func (q Query) After(key string, value *time.Time) Query {
	q.l.After.Key = key
	q.l.After.Value = value
	return q
}

func (q Query) get() internal.Get {
	return internal.Get{
		Table:     q.l.Table,
		Key:       q.l.Key,
		Filter:    q.l.Filter,
		Fields:    q.l.Fields,
		FieldFunc: q.l.FieldFunc,
	}
}
