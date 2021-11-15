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

// Package datastore provides a repository implementation.
package datastore

import (
	"reflect"
	"strings"

	"github.com/pghq/go-museum/museum/diagnostic/errors"

	"github.com/pghq/go-datastore/datastore/client"
)

// Repository is an instance of a postgres Database
type Repository struct {
	client client.Client
}

// New creates a new postgres database
func New(client client.Client) (*Repository, error) {
	if client == nil {
		return nil, errors.New("no database client provided")
	}

	if err := client.Connect(); err != nil {
		return nil, errors.Wrap(err)
	}

	r := &Repository{
		client: client,
	}

	return r, nil
}

// Fields gets all fields for datastore item(s)
func Fields(args ...interface{}) []string {
	var fields []string
	for _, arg := range args {
		switch v := arg.(type) {
		case []string:
			if len(v) > 0 {
				return v
			}
		case string:
			fields = append(fields, v)
		default:
			if i, err := Item(v, true); err == nil {
				for field, _ := range i {
					fields = append(fields, field)
				}
			}
		}
	}

	return fields
}

// Item converts a struct to a map using reflection
// variation of: https://play.golang.org/p/2Qi3thFf--
func Item(in interface{}, transient ...interface{}) (map[string]interface{}, error) {
	if v, ok := in.(map[string]interface{}); ok {
		return v, nil
	}

	v := reflect.ValueOf(in)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if v.Kind() != reflect.Struct {
		return nil, errors.Newf("item of type %T is not a struct", v)
	}

	item := make(map[string]interface{})
	t := v.Type()
	for i := 0; i < v.NumField(); i++ {
		field := t.Field(i)
		if key := field.Tag.Get("db"); key != "" && key != "-" {
			if len(transient) == 0 && strings.HasSuffix(key, ",transient") {
				continue
			}

			item[strings.Split(key, ",")[0]] = v.Field(i).Interface()
		}
	}

	return item, nil
}
