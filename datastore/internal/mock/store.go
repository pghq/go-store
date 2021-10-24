package mock

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/pghq/go-datastore/datastore"
)

var (
	_ datastore.Client  = NewStore(nil)
	_ datastore.Snapper = NewSnapper(nil)
	_ datastore.Cursor  = NewCursor(nil)
)

// Store is a mock datastore.Client
type Store struct {
	Mock
	t    *testing.T
	fail func(v ...interface{})
}

func (s *Store) Connect() error {
	s.t.Helper()
	res := s.Call(s.t)
	if len(res) != 1 {
		s.fail(s.t, "unexpected length of return values")
		return nil
	}

	if res[0] != nil{
		err, ok := res[0].(error)
		if !ok {
			s.fail(s.t, "unexpected type of return value")
			return nil
		}
		return err
	}

	return nil
}

// NewDisconnectedStore creates a new disconnected mock store
func NewDisconnectedStore(t *testing.T) *Store {
	s := Store{
		t: t,
	}

	if t != nil {
		s.fail = t.Fatal
	}

	return &s
}

// NewStore creates a connected mock store
func NewStore(t *testing.T) *Store {
	s := NewDisconnectedStore(t)
	s.Expect("Connect").Return(nil)

	return s
}

// NewDisconnectedStoreWithFail creates a disconnected store with an expected failure
func NewDisconnectedStoreWithFail(t *testing.T, expect ...interface{}) *Store {
	s := NewDisconnectedStore(t)
	s.fail = func(v ...interface{}) {
		t.Helper()
		assert.Equal(t, append([]interface{}{t}, expect...), v)
	}

	return s
}

// Snapper is a mock datastore.Snapper
type Snapper struct {
	Mock
	t    *testing.T
	fail func(v ...interface{})
}

func (s *Snapper) Snapshot() map[string]interface{} {
	s.t.Helper()
	res := s.Call(s.t)
	if len(res) != 1 {
		s.fail(s.t, "unexpected length of return values")
		return nil
	}

	snapshot, ok := res[0].(map[string]interface{})
	if !ok {
		s.fail(s.t, "unexpected type of return value")
		return nil
	}

	return snapshot
}

// NewSnapper creates a mock snapper
func NewSnapper(t *testing.T) *Snapper {
	s := Snapper{
		t: t,
	}

	if t != nil {
		s.fail = t.Fatal
	}

	return &s
}

// NewSnapperWithFail creates a mock datastore.Snapper with an expected failure
func NewSnapperWithFail(t *testing.T, expect ...interface{}) *Snapper {
	s := NewSnapper(t)
	s.fail = func(v ...interface{}) {
		t.Helper()
		assert.Equal(t, append([]interface{}{t}, expect...), v)
	}

	return s
}

type Cursor struct {
	Mock
	t    *testing.T
	fail func(v ...interface{})
}

func (c *Cursor) Next() bool {
	c.t.Helper()
	res := c.Call(c.t)
	if len(res) != 1 {
		c.fail(c.t, "unexpected length of return values")
		return false
	}

	next, ok := res[0].(bool)
	if !ok {
		c.fail(c.t, "unexpected type of return value")
		return false
	}

	return next
}

func (c *Cursor) Decode(values ...interface{}) error {
	c.t.Helper()
	res := c.Call(c.t, values...)
	if len(res) != 1 {
		c.fail(c.t, "unexpected length of return values")
		return nil
	}

	if res[0] != nil {
		err, ok := res[0].(error)
		if !ok {
			c.fail(c.t, "unexpected type of return value")
			return nil
		}
		return err
	}

	return nil
}

func (c *Cursor) Close() {
	c.t.Helper()
	res := c.Call(c.t)
	if len(res) != 0 {
		c.fail(c.t, "unexpected length of return values")
		return
	}
}

func (c *Cursor) Error() error {
	c.t.Helper()
	res := c.Call(c.t)
	if len(res) != 1 {
		c.fail(c.t, "unexpected length of return values")
		return nil
	}

	if res[0] != nil {
		err, ok := res[0].(error)
		if !ok {
			c.fail(c.t, "unexpected type of return value")
			return nil
		}
		return err
	}

	return nil
}

// NewCursor creates a mock datastore.Cursor
func NewCursor(t *testing.T) *Cursor {
	c := Cursor{
		t: t,
	}

	if t != nil {
		c.fail = t.Fatal
	}

	return &c
}

// NewCursorWithFail creates a mock datastore.Cursor with an expected failure
func NewCursorWithFail(t *testing.T, expect ...interface{}) *Cursor {
	c := NewCursor(t)
	c.fail = func(v ...interface{}) {
		t.Helper()
		assert.Equal(t, append([]interface{}{t}, expect...), v)
	}

	return c
}
