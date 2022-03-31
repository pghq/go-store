package ark

import (
	"github.com/pghq/go-tea/trail"

	"github.com/pghq/go-ark/database"
)

// List Retrieve a listing of values
func (tx Txn) List(table string, query database.Query, v DocumentDecoder) error {
	return v.Decode(func(v interface{}) error {
		span := trail.StartSpan(tx, "database.view")
		defer span.Finish()

		if len(query.Fields) == 0 {
			query.Fields = database.AppendFields(query.Fields, v)
		}

		if query.Limit <= 0 {
			query.Limit = database.DefaultLimit
		}

		key := query.Key(table)
		cv, present := tx.cache.Get(key)
		span.Fields.Set("cache.hit", present)
		if present {
			return database.Copy(cv, v)
		}

		if err := tx.backend.List(span.Context(), table, query, v); err != nil {
			return trail.Stacktrace(err)
		}

		if tx.config.ViewTTL != 0 {
			tx.cache.SetWithTTL(key, v, 1, tx.config.ViewTTL)
		}

		return nil
	})
}
