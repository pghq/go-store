package ark

import (
	"github.com/pghq/go-tea/trail"

	"github.com/pghq/go-ark/database"
)

// Get Retrieve a value
func (tx Txn) Get(table string, query database.Query, v DocumentDecoder) error {
	return v.Decode(func(v interface{}) error {
		if len(query.Fields) == 0 {
			query.Fields = database.AppendFields(query.Fields, v)
		}

		if query.Limit <= 0 {
			query.Limit = database.DefaultLimit
		}

		key := query.Key(table)
		if cv, present := tx.cache.Get(key); present {
			return database.Copy(cv, v)
		}

		if err := tx.backend.Get(table, query, v); err != nil {
			return trail.Stacktrace(err)
		}

		if tx.config.ViewTTL != 0 {
			tx.cache.SetWithTTL(key, v, 1, tx.config.ViewTTL)
		}

		return nil
	})
}
