package sql

import (
	"context"
	"database/sql"
	"net/url"
	"reflect"

	"github.com/georgysavva/scany/pgxscan"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	_ "github.com/lib/pq"
	"github.com/pghq/go-tea"

	"github.com/pghq/go-ark/database"
)

// pg backend
type pg struct {
	conn  *sql.DB
	connx *pgxpool.Pool
	ph    placeholder
	url   *url.URL
}

func (p pg) Ping(ctx context.Context) error {
	return p.connx.Ping(ctx)
}

func (p pg) URL() *url.URL {
	return p.url
}

func (p pg) Txn(ctx context.Context, opts *sql.TxOptions) (uow, error) {
	am := pgx.ReadWrite
	if opts.ReadOnly {
		am = pgx.ReadOnly
	}
	txx, err := p.connx.BeginTx(ctx, pgx.TxOptions{AccessMode: am})
	return pgTxn{txx: txx}, err
}

func (p pg) SQL() *sql.DB {
	return p.conn
}

func (p pg) placeholder() placeholder {
	return p.ph
}

// newPostgres creates a new postgres backend
func newPostgres(dialect string, databaseURL *url.URL, config database.Config) (db, error) {
	var err error
	p := pg{ph: "$"}
	p.conn, err = config.SQLOpenFunc(dialect, databaseURL.String())
	if err == nil {
		p.url = databaseURL
		p.connx, err = pgxpool.Connect(context.Background(), databaseURL.String())
	}

	return p, err
}

// pgTxn transaction for postgres
type pgTxn struct {
	txx pgx.Tx
}

func (p pgTxn) Commit(ctx context.Context) error {
	return p.txx.Commit(ctx)
}

func (p pgTxn) Rollback(ctx context.Context) error {
	return p.txx.Rollback(ctx)
}

func (p pgTxn) Get(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	err := pgxscan.Get(ctx, p.txx, dest, query, args...)
	if err != nil {
		var icv *pgconn.PgError
		if tea.AsError(err, &icv) {
			if pgerrcode.IsSyntaxErrororAccessRuleViolation(icv.Code) {
				err = tea.AsErrBadRequest(icv)
			}
		}
	}

	if tea.IsError(err, pgx.ErrNoRows) {
		err = tea.AsErrNotFound(err)
	}
	return err
}

func (p pgTxn) List(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	err := pgxscan.Select(ctx, p.txx, dest, query, args...)
	rv := reflect.ValueOf(dest)
	for {
		if rv.Kind() == reflect.Ptr {
			rv = reflect.Indirect(rv)
			continue
		}
		break
	}

	if err != nil {
		var icv *pgconn.PgError
		if tea.AsError(err, &icv) {
			if pgerrcode.IsSyntaxErrororAccessRuleViolation(icv.Code) {
				err = tea.AsErrBadRequest(icv)
			}
		}
	}

	if err == nil && rv.IsNil() {
		err = tea.ErrNoContent("no content")
	}

	return err
}

func (p pgTxn) Exec(ctx context.Context, query string, args ...interface{}) error {
	_, err := p.txx.Exec(ctx, query, args...)
	if err != nil {
		var icv *pgconn.PgError
		if tea.AsError(err, &icv) {
			if pgerrcode.IsIntegrityConstraintViolation(icv.Code) {
				err = tea.AsErrBadRequest(icv)
			}
		}
	}
	return err
}
