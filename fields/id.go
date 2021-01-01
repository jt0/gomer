package fields

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/jt0/gomer/gomerr"
)

func (f *field) idTag(idTag string) gomerr.Gomerr {
	if idTag == "" {
		return nil
	}

	parts := strings.Split(idTag, ",")
	if len(parts) > 2 {
		return gomerr.Configuration("Expected format '(<defaultTagValue>)?(,<externalName>)?', but got: " + idTag)
	}

	f.defaultTag(parts[0])

	if len(parts) == 2 {
		f.externalName = strings.TrimSpace(parts[1])
	}

	return nil
}

func (fs *Fields) Id(v reflect.Value) (string, gomerr.Gomerr) {
	idv := v.FieldByName(fs.idField.name)
	if !idv.IsValid() {
		return "", gomerr.Unprocessable("Id field", fs.idField.name).AddAttribute("Type", v.Type().Name())
	}
	switch id := idv.Interface().(type) {
	case string:
		return id, nil
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", id), nil
	case fmt.Stringer:
		return id.String(), nil
	default:
		return idv.String(), nil
	}
}
