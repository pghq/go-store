package mock

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/pghq/go-datastore/datastore"
)

var (
	_ datastore.Query = NewQuery(nil)
)

func (s *Store) Query() datastore.Query {
	s.t.Helper()
	res := s.Call(s.t)
	if len(res) != 1 {
		s.fail(s.t, "unexpected length of return values")
		return nil
	}

	query, ok := res[0].(datastore.Query)
	if !ok {
		s.fail(s.t, "unexpected type of return value")
		return nil
	}

	return query
}

// Query is a mock datastore.Query
type Query struct {
	Mock
	t    *testing.T
	fail func(v ...interface{})
}

func (q *Query) Statement() (string, []interface{}, error) {
	q.t.Helper()
	res := q.Call(q.t)
	if len(res) != 3 {
		q.fail(q.t, "unexpected length of return values")
		return "", nil, nil
	}

	if res[2] != nil {
		err, ok := res[2].(error)
		if !ok {
			q.fail(q.t, "unexpected type of return value")
			return "", nil, nil
		}
		return "", nil, err
	}

	statement, ok := res[0].(string)
	if !ok {
		q.fail(q.t, "unexpected type of return value")
		return "", nil, nil
	}

	if res[1] != nil {
		args, ok := res[1].([]interface{})
		if !ok {
			q.fail(q.t, "unexpected type of return value")
			return "", nil, nil
		}
		return statement, args, nil
	}

	return statement, nil, nil
}

func (q *Query) Secondary() datastore.Query {
	q.t.Helper()
	res := q.Call(q.t)
	if len(res) != 1 {
		q.fail(q.t, "unexpected length of return values")
		return nil
	}

	query, ok := res[0].(datastore.Query)
	if !ok {
		q.fail(q.t, "unexpected type of return value")
		return nil
	}

	return query
}

func (q *Query) From(collection string) datastore.Query {
	q.t.Helper()
	res := q.Call(q.t, collection)
	if len(res) != 1 {
		q.fail(q.t, "unexpected length of return values")
		return nil
	}

	query, ok := res[0].(datastore.Query)
	if !ok {
		q.fail(q.t, "unexpected type of return value")
		return nil
	}

	return query
}

func (q *Query) And(collection string, args ...interface{}) datastore.Query {
	q.t.Helper()
	res := q.Call(q.t, append([]interface{}{collection}, args...)...)
	if len(res) != 1 {
		q.fail(q.t, "unexpected length of return values")
		return nil
	}

	query, ok := res[0].(datastore.Query)
	if !ok {
		q.fail(q.t, "unexpected type of return value")
		return nil
	}

	return query
}

func (q *Query) Filter(filter datastore.Filter) datastore.Query {
	q.t.Helper()
	res := q.Call(q.t, filter)
	if len(res) != 1 {
		q.fail(q.t, "unexpected length of return values")
		return nil
	}

	query, ok := res[0].(datastore.Query)
	if !ok {
		q.fail(q.t, "unexpected type of return value")
		return nil
	}

	return query
}

func (q *Query) Order(by string) datastore.Query {
	q.t.Helper()
	res := q.Call(q.t, by)
	if len(res) != 1 {
		q.fail(q.t, "unexpected length of return values")
		return nil
	}

	query, ok := res[0].(datastore.Query)
	if !ok {
		q.fail(q.t, "unexpected type of return value")
		return nil
	}

	return query
}

func (q *Query) First(first int) datastore.Query {
	q.t.Helper()
	res := q.Call(q.t, first)
	if len(res) != 1 {
		q.fail(q.t, "unexpected length of return values")
		return nil
	}

	query, ok := res[0].(datastore.Query)
	if !ok {
		q.fail(q.t, "unexpected type of return value")
		return nil
	}

	return query
}

func (q *Query) After(key string, value interface{}) datastore.Query {
	q.t.Helper()
	res := q.Call(q.t, key, value)
	if len(res) != 1 {
		q.fail(q.t, "unexpected length of return values")
		return nil
	}

	query, ok := res[0].(datastore.Query)
	if !ok {
		q.fail(q.t, "unexpected type of return value")
		return nil
	}

	return query
}

func (q *Query) Return(key string, args ...interface{}) datastore.Query {
	q.t.Helper()
	res := q.Call(q.t, append([]interface{}{key}, args...)...)
	if len(res) != 1 {
		q.fail(q.t, "unexpected length of return values")
		return nil
	}

	query, ok := res[0].(datastore.Query)
	if !ok {
		q.fail(q.t, "unexpected type of return value")
		return nil
	}

	return query
}

func (q *Query) Execute(ctx context.Context) (datastore.Cursor, error) {
	q.t.Helper()
	res := q.Call(q.t, ctx)
	if len(res) != 2 {
		q.fail(q.t, "unexpected length of return values")
		return nil, nil
	}

	if res[1] != nil {
		err, ok := res[1].(error)
		if !ok {
			q.fail(q.t, "unexpected type of return value")
			return nil, nil
		}
		return nil, err
	}

	cursor, ok := res[0].(datastore.Cursor)
	if !ok {
		q.fail(q.t, "unexpected type of return value")
		return nil, nil
	}

	return cursor, nil
}

// NewQuery creates a mock datastore.Query
func NewQuery(t *testing.T) *Query {
	q := Query{
		t: t,
	}

	if t != nil {
		q.fail = t.Fatal
	}

	return &q
}

// NewQueryWithFail creates a mock datastore.Query with an expected failure
func NewQueryWithFail(t *testing.T, expect ...interface{}) *Query {
	q := NewQuery(t)
	q.fail = func(v ...interface{}) {
		t.Helper()
		assert.Equal(t, append([]interface{}{t}, expect...), v)
	}

	return q
}
