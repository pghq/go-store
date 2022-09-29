package pgtest

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/pghq/go-tea/trail"
)

// Start a test database
func Start() (string, func() error, error) {
	pool, err := dockertest.NewPool("")
	opts := dockertest.RunOptions{
		Repository: "postgres",
		Tag:        "12",
		Env: []string{
			"POSTGRES_USER=postgres",
			"POSTGRES_PASSWORD=secret",
			"POSTGRES_DB=db",
			"listen_addresses='*'",
		},
	}

	var resource *dockertest.Resource
	if err == nil {
		resource, err = pool.RunWithOptions(&opts, func(config *docker.HostConfig) {
			config.AutoRemove = true
			config.RestartPolicy = docker.RestartPolicy{Name: "no"}
		})
	}

	if err == nil {
		err = resource.Expire(60)
	}

	var dsn string
	var cleanup func() error
	var conn *sql.DB

	if err == nil {
		pool.MaxWait = 60 * time.Second
		dsn = fmt.Sprintf("postgres://postgres:secret@%s/db?sslmode=disable", resource.GetHostPort("5432/tcp"))
		conn, err = sql.Open("pgx", dsn)
	}

	if err == nil {
		err = pool.Retry(conn.Ping)
		cleanup = func() error {
			return pool.Purge(resource)
		}
	}

	return dsn, cleanup, trail.Stacktrace(err)
}
