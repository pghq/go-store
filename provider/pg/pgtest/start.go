package pgtest

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
)

// Start a test database
func Start() (string, func() error) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		panic(err)
	}

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

	resource, err := pool.RunWithOptions(&opts, func(config *docker.HostConfig) {
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{Name: "no"}
	})

	if err != nil {
		panic(err)
	}

	if err = resource.Expire(60); err != nil {
		panic(err)
	}

	pool.MaxWait = 60 * time.Second
	dsn := fmt.Sprintf("postgres://postgres:secret@%s/db?sslmode=disable", resource.GetHostPort("5432/tcp"))
	conn, err := sql.Open("pgx", dsn)
	if err != nil {
		panic(err)
	}

	if err = pool.Retry(conn.Ping); err != nil {
		panic(err)
	}

	return dsn, func() error {
		return pool.Purge(resource)
	}
}
