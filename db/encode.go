package db

import (
	"bytes"
	"encoding/gob"
	"reflect"

	"github.com/pghq/go-tea"
)

// Encode | Encode a value to bytes
func Encode(v interface{}) ([]byte, error) {
	return GobEncode(v)
}

// Decode | Decode bytes to a value
func Decode(b []byte, v interface{}) error {
	return GobDecode(b, v)
}

// Hash | Hash arguments
func Hash(args ...interface{}) ([]byte, error) {
	return GobHash(args...)
}

// GobEncode | Encode a value to bytes (using gob)
func GobEncode(v interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(v); err != nil {
		return nil, tea.Error(err)
	}
	return buf.Bytes(), nil
}

// GobDecode | Decode bytes to a value (using gob)
func GobDecode(b []byte, v interface{}) error {
	dec := gob.NewDecoder(bytes.NewReader(b))
	rvp, ok := v.(*reflect.Value)
	if !ok {
		rv := reflect.ValueOf(v)
		rvp = &rv
	}
	return dec.DecodeValue(*rvp)
}

// GobHash | Hash arguments to bytes (using gob)
func GobHash(args ...interface{}) ([]byte, error) {
	var b []byte
	for _, v := range args {
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		if err := enc.Encode(v); err != nil {
			return nil, tea.Error(err)
		}
		b = append(b, buf.Bytes()...)
	}

	return b, nil
}
