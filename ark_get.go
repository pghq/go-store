package ark

import (
	"fmt"

	"github.com/pghq/go-ark/database"
	"github.com/pghq/go-tea/trail"
)

// Get Retrieve a value
func (tx Txn) Get(table string, query database.Query, v interface{}) error {
	span := trail.StartSpan(tx.Context(), "Database.Get")
	defer span.Finish()

	decoder, ok := v.(DocumentDecoder)
	if !ok {
		decoder = newTransientDocument(v)
	}

	return decoder.Decode(func(v interface{}) error {
		if len(query.Fields) == 0 {
			query.Fields = database.AppendFields(query.Fields, v)
		}

		if query.Limit == 0 {
			query.Limit = 1
		}

		key := query.Key(table)
		cv, present := tx.cache.Get(key)
		span.Tags.Set("CacheHit", fmt.Sprintf("%t", present))
		if present {
			return database.Copy(cv, v)
		}

		if err := tx.backend.Get(span.Context(), table, query, v); err != nil {
			return trail.Stacktrace(err)
		}

		if tx.config.ViewTTL != 0 {
			tx.cache.SetWithTTL(key, v, 1, tx.config.ViewTTL)
		}

		return nil
	})
}
