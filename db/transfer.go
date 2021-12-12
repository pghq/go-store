package db

import (
	"reflect"
	"strings"

	"github.com/pghq/go-tea"
)

// Map | Convert a struct (w. optional tags) to a map using reflection
// variation of: https://play.golang.org/p/2Qi3thFf--
// meant to be used for data persistence.
func Map(in interface{}, transient ...interface{}) (map[string]interface{}, error) {
	if v, ok := in.(map[string]interface{}); ok {
		return v, nil
	}

	v := reflect.ValueOf(in)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if v.Kind() != reflect.Struct {
		return nil, tea.NewErrorf("item of type %T is not a struct", v)
	}

	item := make(map[string]interface{})
	t := v.Type()
	for i := 0; i < v.NumField(); i++ {
		sf := t.Field(i)
		key := sf.Tag.Get("db")
		if key == "" {
			key = sf.Name
		}

		if key == "-" || len(transient) == 0 && strings.HasSuffix(key, ",transient") {
			continue
		}

		item[strings.Split(key, ",")[0]] = v.Field(i).Interface()
	}

	return item, nil
}

// Copy | Copy src value to destination
func Copy(src, dst interface{}) error {
	dv := reflect.Indirect(reflect.ValueOf(dst))
	if !dv.CanSet() {
		return tea.NewError("bad destination")
	}

	sv := reflect.Indirect(reflect.ValueOf(src))
	dv.Set(sv)
	return nil
}
