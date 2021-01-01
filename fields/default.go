package fields

import (
	"reflect"
	"strings"

	"github.com/jt0/gomer/flect"
	"github.com/jt0/gomer/gomerr"
)

type DefaultFunction func(reflect.Value) interface{}

func RegisterDefaultFunctions(functions map[string]DefaultFunction) {
	for fnName := range functions {
		if ge := dollarNameConstraint.Validate(fnName); ge != nil {
			panic(ge.AddAttribute("Issue", "Function names must start with a '$' symbol and between 2 and 64 characters long"))
		}

		if fnName[1:2] == "_" {
			panic("Function names first name character must not an underscore")
		}
	}

	registeredDefaultFunctions = functions
}

var registeredDefaultFunctions map[string]DefaultFunction

func (f *field) defaultTag(defaultTag string) {
	if defaultTag == "" {
		return
	}

	f.defaultValues = make(map[string]defaultValue)
	for _, contextDefault := range strings.Split(defaultTag, ";") {
		contextDefault = strings.TrimSpace(contextDefault)

		var context, valueText string
		if separatorIndex := strings.Index(contextDefault, ":"); separatorIndex <= 0 {
			context = matchImplicitly
			valueText = contextDefault
		} else {
			context = contextDefault[:separatorIndex]
			valueText = contextDefault[separatorIndex+1:]
		}

		dv := defaultValue{}

		if valueText[:1] == "?" {
			dv.bypassIfSet = true
			valueText = valueText[1:]
		}

		if valueText[:1] != "$" {
			dv.value = valueText
		} else if fn, ok := registeredDefaultFunctions[valueText]; ok {
			dv.function = fn
		} else {
			dv.value = valueText
		}

		f.defaultValues[context] = dv
	}
}

func (fs *Fields) ApplyDefaults(v reflect.Value, context string) gomerr.Gomerr {
	// TODO: handle nested/embedded structs
	for _, field := range fs.fieldMap {
		defaultValue, ok := field.defaultValues[context]
		if !ok {
			if defaultValue, ok = field.defaultValues[matchImplicitly]; !ok {
				continue
			}
		}

		if ge := defaultValue.apply(v, field.name); ge != nil {
			return ge
		}
	}

	return nil
}

func (dv defaultValue) apply(v reflect.Value, name string) gomerr.Gomerr {
	fv := v.FieldByName(name)
	if !fv.IsValid() || (dv.bypassIfSet && flect.IsSet(fv)) {
		return nil
	}

	var val interface{}
	if dv.function != nil {
		val = dv.function(v)
	} else {
		val = dv.value
	}

	if ge := flect.SetValue(fv, val); ge != nil {
		return gomerr.Configuration("Unable to set field to default value").AddAttributes("Field", name, "Value", val).Wrap(ge)
	}

	return nil
}
