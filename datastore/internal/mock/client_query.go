package mock

import (
	"testing"
	"time"

	"github.com/pghq/go-datastore/datastore/client"
)

var (
	_ client.Query = NewQuery(nil)
)

func (c *Client) Query() client.Query {
	c.t.Helper()
	res := c.Call(c.t)
	if len(res) != 1 {
		c.fail(c.t, "unexpected length of return values")
		return nil
	}

	query, ok := res[0].(client.Query)
	if !ok {
		c.fail(c.t, "unexpected type of return value")
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

func (q *Query) Secondary() client.Query {
	q.t.Helper()
	res := q.Call(q.t)
	if len(res) != 1 {
		q.fail(q.t, "unexpected length of return values")
		return nil
	}

	query, ok := res[0].(client.Query)
	if !ok {
		q.fail(q.t, "unexpected type of return value")
		return nil
	}

	return query
}

func (q *Query) From(collection string) client.Query {
	q.t.Helper()
	res := q.Call(q.t, collection)
	if len(res) != 1 {
		q.fail(q.t, "unexpected length of return values")
		return nil
	}

	query, ok := res[0].(client.Query)
	if !ok {
		q.fail(q.t, "unexpected type of return value")
		return nil
	}

	return query
}

func (q *Query) Complement(collection string, args ...interface{}) client.Query {
	q.t.Helper()
	res := q.Call(q.t, append([]interface{}{collection}, args...)...)
	if len(res) != 1 {
		q.fail(q.t, "unexpected length of return values")
		return nil
	}

	query, ok := res[0].(client.Query)
	if !ok {
		q.fail(q.t, "unexpected type of return value")
		return nil
	}

	return query
}

func (q *Query) Filter(filter interface{}) client.Query {
	q.t.Helper()
	res := q.Call(q.t, filter)
	if len(res) != 1 {
		q.fail(q.t, "unexpected length of return values")
		return nil
	}

	query, ok := res[0].(client.Query)
	if !ok {
		q.fail(q.t, "unexpected type of return value")
		return nil
	}

	return query
}

func (q *Query) Order(by string) client.Query {
	q.t.Helper()
	res := q.Call(q.t, by)
	if len(res) != 1 {
		q.fail(q.t, "unexpected length of return values")
		return nil
	}

	query, ok := res[0].(client.Query)
	if !ok {
		q.fail(q.t, "unexpected type of return value")
		return nil
	}

	return query
}

func (q *Query) First(first int) client.Query {
	q.t.Helper()
	res := q.Call(q.t, first)
	if len(res) != 1 {
		q.fail(q.t, "unexpected length of return values")
		return nil
	}

	query, ok := res[0].(client.Query)
	if !ok {
		q.fail(q.t, "unexpected type of return value")
		return nil
	}

	return query
}

func (q *Query) After(key string, value *time.Time) client.Query {
	q.t.Helper()
	res := q.Call(q.t, key, value)
	if len(res) != 1 {
		q.fail(q.t, "unexpected length of return values")
		return nil
	}

	query, ok := res[0].(client.Query)
	if !ok {
		q.fail(q.t, "unexpected type of return value")
		return nil
	}

	return query
}

func (q *Query) Fields(fields ...interface{}) client.Query {
	q.t.Helper()
	res := q.Call(q.t, fields...)
	if len(res) != 1 {
		q.fail(q.t, "unexpected length of return values")
		return nil
	}

	query, ok := res[0].(client.Query)
	if !ok {
		q.fail(q.t, "unexpected type of return value")
		return nil
	}

	return query
}

func (q *Query) Field(key string, args ...interface{}) client.Query {
	q.t.Helper()
	res := q.Call(q.t, append([]interface{}{key}, args...)...)
	if len(res) != 1 {
		q.fail(q.t, "unexpected length of return values")
		return nil
	}

	query, ok := res[0].(client.Query)
	if !ok {
		q.fail(q.t, "unexpected type of return value")
		return nil
	}

	return query
}

func (q *Query) Transform(transform func(string) string) client.Query {
	q.t.Helper()
	res := q.Call(q.t, transform)
	if len(res) != 1 {
		q.fail(q.t, "unexpected length of return values")
		return nil
	}

	query, ok := res[0].(client.Query)
	if !ok {
		q.fail(q.t, "unexpected type of return value")
		return nil
	}

	return query
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
