package fields

import (
	"reflect"
	"regexp"
	"strings"

	"github.com/jt0/gomer/gomerr"
)

type RegexpWrapper struct {
	Regexp       *regexp.Regexp
	RegexpGroups []string // index = regex group number - 1; value = key (e.g. "Name", "location", nil)
	FieldTool
}

func (w RegexpWrapper) Name() string {
	return w.FieldTool.Name()
}

func (w RegexpWrapper) New(structType reflect.Type, structField reflect.StructField, input interface{}) (FieldTool, gomerr.Gomerr) {
	inputString, ok := input.(string)
	if !ok {
		inputString = ""
	}

	valuesList := make([]map[string]string, 0)
	for _, match := range w.Regexp.FindAllStringSubmatch(inputString, -1) {
		values := make(map[string]string)
		for i, value := range match {
			key := w.RegexpGroups[i]
			if key == "" {
				continue
			}
			values[key] = strings.TrimSpace(value)
		}

		valuesList = append(valuesList, values)
	}

	return w.FieldTool.New(structType, structField, valuesList)
}
