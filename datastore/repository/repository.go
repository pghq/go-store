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

// Package repository provides a collection like object for persistence.
package repository

import (
	"github.com/pghq/go-museum/museum/diagnostic/errors"

	"github.com/pghq/go-datastore/datastore"
)

// Repository is an instance of a postgres Database
type Repository struct {
	client datastore.Client
}

// Filter gets a new filter for searching the repository.
func (r *Repository) Filter() datastore.Filter {
	return r.client.Filter()
}

// New creates a new postgres database
func New(client datastore.Client) (*Repository, error) {
	if client == nil {
		return nil, errors.New("no database client provided")
	}

	if err := client.Connect(); err != nil {
		return nil, errors.Wrap(err)
	}

	r := Repository{
		client: client,
	}

	return &r, nil
}
