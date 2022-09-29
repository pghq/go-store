package enc

import (
	"reflect"
	"strings"

	"github.com/gobeam/stringy"
)

// Map Convert an interface to a map using reflection
// variation of: https://play.golang.org/p/2Qi3thFf--
// meant to be used for data persistence.
func Map(v interface{}, ignoreKeys ...string) map[string]interface{} {
	if m, ok := v.(map[string]interface{}); ok || v == nil {
		return m
	}

	if m, ok := v.(*map[string]interface{}); ok {
		return *m
	}

	rv := reflect.Indirect(reflect.ValueOf(v))
	for {
		if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
			rv = reflect.Zero(rv.Type().Elem())
		}

		if rv.Kind() != reflect.Ptr {
			break
		}

		rv = reflect.Indirect(rv)
	}

	if rv.Kind() != reflect.Struct {
		return nil
	}

	item := make(map[string]interface{})
	t := rv.Type()
	ignore := make(map[string]struct{}, len(ignoreKeys))
	for _, key := range ignoreKeys {
		ignore[key] = struct{}{}
	}

	for i := 0; i < rv.NumField(); i++ {
		sf := t.Field(i)
		key := sf.Tag.Get("db")
		if key == "" {
			key = stringy.New(sf.Name).SnakeCase().ToLower()
		}

		_, ignoreKey := ignore[key]
		if key == "-" || ignoreKey {
			continue
		}

		item[strings.Split(key, ",")[0]] = rv.Field(i).Interface()
	}

	return item
}
