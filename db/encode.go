package db

import (
	"bytes"
	"encoding/gob"
	"reflect"

	"github.com/pghq/go-tea"
)

// Encode Encode a value to bytes
func Encode(v interface{}) ([]byte, error) {
	return GobEncode(v)
}

// Decode Decode bytes to a value
func Decode(b []byte, v interface{}) error {
	return GobDecode(b, v)
}

// GobEncode Encode a value to bytes (using gob)
func GobEncode(v interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(v); err != nil {
		return nil, tea.Error(err)
	}
	return buf.Bytes(), nil
}

// GobDecode Decode bytes to a value (using gob)
func GobDecode(b []byte, v interface{}) error {
	dec := gob.NewDecoder(bytes.NewReader(b))
	rvp, ok := v.(*reflect.Value)
	if !ok {
		rv := reflect.ValueOf(v)
		rvp = &rv
	}
	return dec.DecodeValue(*rvp)
}
