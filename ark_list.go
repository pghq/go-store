package ark

import (
	"fmt"

	"github.com/pghq/go-ark/database"
	"github.com/pghq/go-tea/trail"
)

// List Retrieve a listing of values
func (tx Txn) List(table string, query database.Query, v interface{}) error {
	span := trail.StartSpan(tx, "database.list")
	defer span.Finish()

	decoder, ok := v.(DocumentDecoder)
	if !ok {
		decoder = newTransientDocument(v)
	}

	return decoder.Decode(span.Context(), func(v interface{}) error {
		if len(query.Fields) == 0 {
			query.Fields = database.AppendFields(query.Fields, v)
		}

		if query.Limit <= 0 {
			query.Limit = database.DefaultLimit
		}

		key := query.Key(table)
		cv, present := tx.cache.Get(key)
		span.Tags.Set("cache.hit", fmt.Sprintf("%t", present))
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
