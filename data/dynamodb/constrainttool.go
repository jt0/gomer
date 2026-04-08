package dynamodb

import (
	"context"
	"reflect"
	"regexp"
	"strings"

	"github.com/jt0/gomer/constraint"
	"github.com/jt0/gomer/data"
	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/structs"
)

// NewConstraintTool creates a tool for validating db.constraints
func NewConstraintTool(t *table) *structs.Tool {
	return structs.NewTool(
		"dynamodb.ConstraintValidation",
		constraintApplierProvider{table: t},
		structs.StructTagDirectiveProvider{TagKey: "db.constraints"},
	)
}

type constraintApplierProvider struct {
	table *table
}

// constraintsRegexp matches: unique, unique(), unique(Field1), unique(Field1,Field2)
var constraintsRegexp = regexp.MustCompile(`(unique)(\(([\w,]+)\))?`)

func (ap constraintApplierProvider) Applier(_ reflect.Type, sf reflect.StructField, directive string, _ string) (structs.Applier, gomerr.Gomerr) {
	if directive == "" {
		return nil, nil
	}

	// Parse directive: "unique(Field1,Field2)"
	matches := constraintsRegexp.FindAllStringSubmatch(directive, -1)
	if matches == nil {
		return nil, gomerr.Configuration("invalid db.constraints value: "+directive).AddAttribute("field", sf.Name)
	}

	for _, match := range matches {
		if match[1] == "unique" {
			// Build field tuple: [currentField, additionalField1, additionalField2, ...]
			fieldTuple := []string{sf.Name}
			if match[3] != "" {
				additionalFields := strings.Split(strings.ReplaceAll(match[3], " ", ""), ",")
				fieldTuple = append(fieldTuple, additionalFields...)
			}

			t := ap.table
			return uniquenessApplier{
				constraint.New("unique", fieldTuple, func(a any) gomerr.Gomerr {
					tt := a.(toTest)
					return t.checkFieldTupleUnique(tt.ctx, tt.p, fieldTuple)
				}),
			}, nil
		}
	}

	return nil, nil
}

type toTest struct {
	ctx context.Context
	p   data.Persistable
}

type uniquenessApplier struct {
	constraint constraint.Constraint
}

func (a uniquenessApplier) Apply(sv reflect.Value, _ reflect.Value, tc structs.ToolContext) gomerr.Gomerr {
	// Get context from ToolContext
	ctxVal := tc.Get("ctx")
	if ctxVal == nil {
		return gomerr.Configuration("context.Context not found in ToolContext")
	}
	ctx := ctxVal.(context.Context)

	// Get the persistable (sv is the struct value)
	p, ok := sv.Addr().Interface().(data.Persistable)
	if !ok {
		return gomerr.Configuration("struct does not implement data.Persistable").AddAttribute("type", sv.Type().String())
	}

	return a.constraint.Test(toTest{ctx, p})
}
