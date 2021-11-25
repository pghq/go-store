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

// Package client provides a shared interface for database impls.
package client

import (
	"context"
	"fmt"
	"time"
)

// Client represents a client for operating on a database.
type Client interface {
	Connect() error
	Query() Query
	Add() Add
	Update() Update
	Remove() Remove
	Transaction(ctx context.Context, ro bool) (Transaction, error)
}

// Transaction represents a database transaction.
type Transaction interface {
	Execute(statement Encoder, dst ...interface{}) (int, error)
	Commit() error
	Rollback() error
}

// Add represents a command to add items to the collection
type Add interface {
	Encoder
	To(collection string) Add
	Item(value map[string]interface{}) Add
	Query(query Query) Add
}

// Update represents a command to update items in the collection
type Update interface {
	Encoder
	In(collection string) Update
	Item(value map[string]interface{}) Update
	Filter(filter interface{}) Update
}

// Remove represents a command to remove items from the collection
type Remove interface {
	Encoder
	From(collection string) Remove
	Filter(filter interface{}) Remove
	Order(by string) Remove
	After(key string, value *time.Time) Remove
}

// Encoder represents a statement encoder
type Encoder interface {
	Statement() (string, []interface{}, error)
}

// Query represents a query builder
type Query interface {
	Encoder
	fmt.Stringer
	From(collection string) Query
	Complement(collection string, args ...interface{}) Query
	Filter(filter interface{}) Query
	Order(by string) Query
	First(first int) Query
	After(key string, value *time.Time) Query
	Transform(transform func(string) string) Query
	Fields(fields ...interface{}) Query
	Field(key string, args ...interface{}) Query
}
