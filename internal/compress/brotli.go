package compress

import (
	"bytes"
	"encoding/gob"
	"reflect"

	"github.com/andybalholm/brotli"
	"github.com/pghq/go-tea"
)

// BrotliDecode value
func BrotliDecode(b []byte, v interface{}) error {
	dec := gob.NewDecoder(brotli.NewReader(bytes.NewReader(b)))
	rvp, ok := v.(*reflect.Value)
	if !ok {
		rv := reflect.ValueOf(v)
		rvp = &rv
	}
	return dec.DecodeValue(*rvp)
}

// BrotliEncode value
func BrotliEncode(v interface{}) ([]byte, error) {
	var buf bytes.Buffer
	w := brotli.NewWriter(&buf)
	enc := gob.NewEncoder(w)
	if err := enc.Encode(v); err != nil {
		return nil, tea.Error(err)
	}

	_ = w.Flush()
	return buf.Bytes(), nil
}

// BrotliHash args
func BrotliHash(args ...interface{}) ([]byte, error) {
	var b []byte
	for _, v := range args {
		var buf bytes.Buffer
		w := brotli.NewWriter(&buf)
		enc := gob.NewEncoder(w)
		if err := enc.Encode(v); err != nil {
			return nil, tea.Error(err)
		}
		_ = w.Flush()
		b = append(b, buf.Bytes()...)
	}

	return b, nil
}
