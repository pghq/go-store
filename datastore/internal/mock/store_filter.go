package mock

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/pghq/go-datastore/datastore"
)

var (
	_ datastore.Filter = NewFilter(nil)
)

func (s *Store) Filter() datastore.Filter {
	s.t.Helper()
	res := s.Call(s.t)
	if len(res) != 1 {
		s.fail(s.t, "unexpected length of return values")
		return nil
	}

	filter, ok := res[0].(datastore.Filter)
	if !ok {
		s.fail(s.t, "unexpected type of return value")
		return nil
	}

	return filter
}

// Filter is a mock datastore.Filter
type Filter struct {
	Mock
	t    *testing.T
	fail func(v ...interface{})
}

func (f *Filter) BeginsWith(key string, prefix string) datastore.Filter {
	f.t.Helper()
	res := f.Call(f.t, key, prefix)
	if len(res) != 1 {
		f.fail(f.t, "unexpected length of return values")
		return nil
	}

	filter, ok := res[0].(datastore.Filter)
	if !ok {
		f.fail(f.t, "unexpected type of return value")
		return nil
	}

	return filter
}

func (f *Filter) EndsWith(key string, suffix string) datastore.Filter {
	f.t.Helper()
	res := f.Call(f.t, key, suffix)
	if len(res) != 1 {
		f.fail(f.t, "unexpected length of return values")
		return nil
	}

	filter, ok := res[0].(datastore.Filter)
	if !ok {
		f.fail(f.t, "unexpected type of return value")
		return nil
	}

	return filter
}

func (f *Filter) Contains(key string, value interface{}) datastore.Filter {
	f.t.Helper()
	res := f.Call(f.t, key, value)
	if len(res) != 1 {
		f.fail(f.t, "unexpected length of return values")
		return nil
	}

	filter, ok := res[0].(datastore.Filter)
	if !ok {
		f.fail(f.t, "unexpected type of return value")
		return nil
	}

	return filter
}

func (f *Filter) NotContains(key string, value interface{}) datastore.Filter {
	f.t.Helper()
	res := f.Call(f.t, key, value)
	if len(res) != 1 {
		f.fail(f.t, "unexpected length of return values")
		return nil
	}

	filter, ok := res[0].(datastore.Filter)
	if !ok {
		f.fail(f.t, "unexpected type of return value")
		return nil
	}

	return filter
}

func (f *Filter) Eq(key string, value interface{}) datastore.Filter {
	f.t.Helper()
	res := f.Call(f.t, key, value)
	if len(res) != 1 {
		f.fail(f.t, "unexpected length of return values")
		return nil
	}

	filter, ok := res[0].(datastore.Filter)
	if !ok {
		f.fail(f.t, "unexpected type of return value")
		return nil
	}

	return filter
}

func (f *Filter) Lt(key string, value interface{}) datastore.Filter {
	f.t.Helper()
	res := f.Call(f.t, key, value)
	if len(res) != 1 {
		f.fail(f.t, "unexpected length of return values")
		return nil
	}

	filter, ok := res[0].(datastore.Filter)
	if !ok {
		f.fail(f.t, "unexpected type of return value")
		return nil
	}

	return filter
}

func (f *Filter) Gt(key string, value interface{}) datastore.Filter {
	f.t.Helper()
	res := f.Call(f.t, key, value)
	if len(res) != 1 {
		f.fail(f.t, "unexpected length of return values")
		return nil
	}

	filter, ok := res[0].(datastore.Filter)
	if !ok {
		f.fail(f.t, "unexpected type of return value")
		return nil
	}

	return filter
}

func (f *Filter) NotEq(key string, value interface{}) datastore.Filter {
	f.t.Helper()
	res := f.Call(f.t, key, value)
	if len(res) != 1 {
		f.fail(f.t, "unexpected length of return values")
		return nil
	}

	filter, ok := res[0].(datastore.Filter)
	if !ok {
		f.fail(f.t, "unexpected type of return value")
		return nil
	}

	return filter
}

func (f *Filter) Or(another datastore.Filter) datastore.Filter {
	f.t.Helper()
	res := f.Call(f.t, another)
	if len(res) != 1 {
		f.fail(f.t, "unexpected length of return values")
		return nil
	}

	filter, ok := res[0].(datastore.Filter)
	if !ok {
		f.fail(f.t, "unexpected type of return value")
		return nil
	}

	return filter
}

func (f *Filter) And(another datastore.Filter) datastore.Filter {
	f.t.Helper()
	res := f.Call(f.t, another)
	if len(res) != 1 {
		f.fail(f.t, "unexpected length of return values")
		return nil
	}

	filter, ok := res[0].(datastore.Filter)
	if !ok {
		f.fail(f.t, "unexpected type of return value")
		return nil
	}

	return filter
}

// NewFilter creates a mock datastore.Filter
func NewFilter(t *testing.T) *Filter {
	f := Filter{
		t: t,
	}

	if t != nil {
		f.fail = t.Fatal
	}

	return &f
}

// NewFilterWithFail creates a mock datastore.Filter with an expected failure
func NewFilterWithFail(t *testing.T, expect ...interface{}) *Filter {
	f := NewFilter(t)
	f.fail = func(v ...interface{}) {
		t.Helper()
		assert.Equal(t, append([]interface{}{t}, expect...), v)
	}

	return f
}
