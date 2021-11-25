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

// Package integration provides resources for doing integration testing.
package integration

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/pghq/go-tea"
	"github.com/stretchr/testify/assert"

	"github.com/pghq/go-ark"
)

const (
	// DefaultContainerTTL is the default ttl for docker containers
	DefaultContainerTTL = time.Minute

	// DefaultMaxConnectTime is the default amount of time to allow connecting
	DefaultMaxConnectTime = 60 * time.Second

	// DefaultTag is the default tag for the postgres docker image
	DefaultTag = "13"

	// DefaultDockerEndpoint is the default docker endpoint for connections
	DefaultDockerEndpoint = ""
)

// Postgres is an integration for running postgres tests using docker
type Postgres struct {
	Store     *ark.Store
	Migration struct {
		FS        fs.FS
		Directory string
	}
	ImageTag       string
	ContainerTTL   time.Duration
	MaxConnectTime time.Duration
	DockerEndpoint string

	exit  func(code int)
	run   func() int
	purge func(r *dockertest.Resource) error
	emit  func(err error)
}

// NewPostgres creates a new integration test for postgres
func NewPostgres(m *testing.M) *Postgres {
	p := Postgres{
		run:  m.Run,
		exit: os.Exit,
		emit: tea.SendError,
	}

	return &p
}

// NewPostgresWithExit creates a new postgres image with an expected exit
func NewPostgresWithExit(t *testing.T, code int) *Postgres {
	p := Postgres{
		run:  func() int { return 0 },
		emit: tea.SendError,
		exit: ExpectExit(t, code),
	}

	return &p
}

// RunPostgres runs a new postgres integration
func RunPostgres(integration *Postgres) {
	if integration.ContainerTTL == 0 {
		integration.ContainerTTL = DefaultContainerTTL
	}

	if integration.ImageTag == "" {
		integration.ImageTag = DefaultTag
	}

	if integration.MaxConnectTime == 0 {
		integration.MaxConnectTime = DefaultMaxConnectTime
	}

	if integration.DockerEndpoint == "" {
		integration.DockerEndpoint = DefaultDockerEndpoint
	}

	pool, err := dockertest.NewPool(integration.DockerEndpoint)
	if err != nil {
		integration.emit(err)
		integration.exit(1)
		return
	}

	pool.MaxWait = integration.MaxConnectTime
	opts := dockertest.RunOptions{
		Repository: "postgres",
		Tag:        integration.ImageTag,
		Env: []string{
			"POSTGRES_USER=test",
			"POSTGRES_PASSWORD=test",
			"POSTGRES_DB=test",
			"listen_addresses='*'",
		},
	}

	resource, err := pool.RunWithOptions(&opts, func(config *docker.HostConfig) {
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{
			Name: "no",
		}
	})

	if err != nil {
		integration.emit(err)
		integration.exit(1)
		return
	}

	// Unfortunately, this method does not do any error handling :(
	_ = resource.Expire(uint(integration.ContainerTTL.Seconds()))
	connect := func() error {
		tea.SetGlobalLogWriter(io.Discard)
		defer tea.ResetGlobalLogger()

		primary := fmt.Sprintf("postgres://test:test@localhost:%s/test?sslmode=disable", resource.GetPort("5432/tcp"))
		integration.Store, err = ark.NewStore(primary, ark.MigrationFS(integration.Migration.FS), ark.MigrationDirectory(integration.Migration.Directory))
		return err
	}

	purge := pool.Purge
	if integration.purge != nil {
		purge = func(r *dockertest.Resource) error {
			_ = pool.Purge(r)
			return integration.purge(resource)
		}
	}

	if deadline := pool.Retry(connect); deadline != nil {
		integration.emit(err)
		_ = purge(resource)
		integration.exit(1)
		return
	}

	code := integration.run()

	if err := purge(resource); err != nil {
		integration.emit(err)
		integration.exit(1)
		return
	}

	integration.exit(code)
}

// ExpectExit is a test function for asserting exit codes when exit is called
func ExpectExit(t *testing.T, expect int) func(code int) {
	return func(code int) {
		assert.Equal(t, expect, code)
	}
}
