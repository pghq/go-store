package ark

// Insert insert a value
func (tx Txn) Insert(table string, v interface{}) error {
	return tx.backend.Insert(table, v)
}
