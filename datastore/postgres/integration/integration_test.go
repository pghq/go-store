//go:build !unit
// +build !unit

package integration

import (
	"io"
	"testing"
	"testing/fstest"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"

	"github.com/pghq/go-museum/museum/diagnostic/errors"
	"github.com/pghq/go-museum/museum/diagnostic/log"
)

func TestPostgres(t *testing.T) {
	t.Run("can create main postgres", func(t *testing.T) {
		test := NewPostgres(&testing.M{})
		assert.NotNil(t, test)
		assert.NotNil(t, test.exit)
		assert.NotNil(t, test.run)
		assert.NotNil(t, test.emit)
	})

	t.Run("raises bad docker endpoint errors", func(t *testing.T) {
		log.Writer(io.Discard)
		defer log.Reset()
		test := NewPostgresWithExit(t, 1)
		test.DockerEndpoint = "https://[::1]:namedport"
		RunPostgres(test)
		assert.Nil(t, test.Repository)
	})

	t.Run("raises bad image tag errors", func(t *testing.T) {
		log.Writer(io.Discard)
		defer log.Reset()
		test := NewPostgresWithExit(t, 1)
		test.ImageTag = "0"
		RunPostgres(test)
		assert.Nil(t, test.Repository)
	})

	t.Run("raises connection errors", func(t *testing.T) {
		log.Writer(io.Discard)
		defer log.Reset()
		fs := fstest.MapFS{
			"migrations/test.sql": &fstest.MapFile{
				Data: []byte("-- +goose Up\nCREATE TABLE IF NOT EXISTS tests (id uuid);"),
			},
		}
		test := NewPostgresWithExit(t, 1)
		test.emit = func(err error) { assert.NotNil(t, err) }
		test.MaxConnectTime = time.Nanosecond
		test.Migration.FS = fs
		test.Migration.Directory = "migrations"
		RunPostgres(test)
		assert.Nil(t, test.Repository)
	})

	t.Run("raises purge errors", func(t *testing.T) {
		log.Writer(io.Discard)
		defer log.Reset()
		test := NewPostgresWithExit(t, 1)
		test.purge = func(r *dockertest.Resource) error {
			return errors.New("an error has occurred")
		}
		test.emit = func(err error) { assert.NotNil(t, err) }
		RunPostgres(test)
	})

	t.Run("has a repository", func(t *testing.T) {
		fs := fstest.MapFS{
			"migrations/00001_test.sql": &fstest.MapFile{
				Data: []byte("-- +goose Up\nCREATE TABLE IF NOT EXISTS tests (id uuid);"),
			},
		}
		test := NewPostgresWithExit(t, 0)
		assert.NotNil(t, test)
		test.ImageTag = "11"
		test.ContainerTTL = time.Minute
		test.Migration.FS = fs
		test.Migration.Directory = "migrations"
		RunPostgres(test)
		assert.NotNil(t, test.Repository)
	})
}
