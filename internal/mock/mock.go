// Copyright 2021 PGHQ. All Rights Reserved.
//
// Licensed under the GNU General Public License, Version 3 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package mock provides a basic mock for testing the datastore.
package mock

import (
	"context"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

type Mock struct {
	expected map[string][]*Expect
	lock     sync.Mutex
}

func (m *Mock) Expect(funcName string, args ...interface{}) *Expect {
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.expected == nil {
		m.expected = make(map[string][]*Expect)
	}

	expect := &Expect{funcName: funcName, input: args}
	m.expected[funcName] = append(m.expected[funcName], expect)

	return expect
}

func (m *Mock) Assert(t *testing.T) {
	m.lock.Lock()
	defer m.lock.Unlock()

	t.Helper()
	for k, _ := range m.expected {
		expectations, ok := m.expected[k]
		if ok && len(expectations) > 0 {
			m.Fatalf(t, "not enough calls to %s, %d missing", k, len(expectations))
			return
		}
	}
}

func (m *Mock) Fatal(t *testing.T, args ...interface{}) {
	t.Helper()
	t.Fatal(args...)
}

func (m *Mock) Fatalf(t *testing.T, format string, args ...interface{}) {
	t.Helper()
	t.Fatalf(format, args...)
}

func (m *Mock) Call(t *testing.T, args ...interface{}) []interface{} {
	m.lock.Lock()
	defer m.lock.Unlock()

	t.Helper()
	pc, _, _, ok := runtime.Caller(1)
	caller := runtime.FuncForPC(pc)
	if ok && caller != nil {
		path := strings.Split(caller.Name()[5:], ".")
		funcName := path[len(path)-1]
		expectations, ok := m.expected[funcName]
		if !ok || len(expectations) == 0 {
			m.Fatalf(t, "unexpected call to %s", funcName)
		}

		expect, expects := expectations[0], expectations[1:]
		if len(expect.input) != len(args) {
			m.Fatalf(t, "arguments %+v to call %s != %+v", args, funcName, expect.input)
		}

		for i, arg := range expect.input {
			if _, ok := arg.(context.Context); ok {
				assert.Implements(t, (*context.Context)(nil), args[i])
			} else if _, ok := arg.(interface {
				Call(t *testing.T, args ...interface{}) []interface{}
			}); ok {
				assert.IsType(t, arg, args[i])
			} else {
				assert.Equal(t, arg, args[i])
			}
		}

		m.expected[funcName] = expects
		return expect.output
	}

	m.Fatal(t, "could not obtain caller information")
	return nil
}

type Expect struct {
	funcName string
	input    []interface{}
	output   []interface{}
}

func (e *Expect) Return(output ...interface{}) {
	e.output = output
}
