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

// Package internal provides our internal API resources.
//
// Any functionality provided by this package should not
// depend on any external packages within the application.
package internal

import "time"

// Clock is an interface representing the internal clock time
type Clock interface {
	Now() time.Time
	Start() time.Time
	From(now func() time.Time) Clock
}
