package dynamodb

import (
	"reflect"
	"regexp"
	"strconv"
	"strings"

	"github.com/jt0/gomer/constraint"
	"github.com/jt0/gomer/data"
	"github.com/jt0/gomer/data/dataerr"
	"github.com/jt0/gomer/fields"
	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/util"
)

const (
	UniqueConstraint = "unique"

	constrainedFieldsKey = "constrainedFields"
	constraintTypeKey    = "ConstraintType"
	additionalFieldsKey  = "AdditionalFields"
)

type collectConstrainedFieldsTool struct {
	t *table
}

func (c collectConstrainedFieldsTool) Name() string {
	panic("implement me")
}

func (c collectConstrainedFieldsTool) New(structType reflect.Type, structField reflect.StructField, input interface{}) (fields.FieldToolApplier, gomerr.Gomerr) {
	panic("implement me")
}

func (c collectConstrainedFieldsTool) Apply(structValue reflect.Value, fieldValue reflect.Value, toolContext fields.ToolContext) gomerr.Gomerr {
	panic("implement me")
}

func newConstraintValidator(table *table) fields.FieldTool {
	return fields.RegexpWrapper{
		Regexp:       regexp.MustCompile("(unique)(\\(([\\w,]+)\\))?"),
		RegexpGroups: []string{"", constraintTypeKey, additionalFieldsKey},
		FieldTool:    constraintValidatorTool{table},
	}
}

type constraintValidatorTool struct {
	table *table
}

func (cvt constraintValidatorTool) Name() string {
	return "ddb.ConstraintValidatorTool"
}

// "db.constraints"
func (cvt constraintValidatorTool) New(_ reflect.Type, structField reflect.StructField, input interface{}) (fields.FieldToolApplier, gomerr.Gomerr) {
	fieldConstraints := input.([]map[string]string)
	if lenFieldConstraints := len(fieldConstraints); lenFieldConstraints == 0 {
		return nil, nil
	} else if lenFieldConstraints > 1 {
		return nil, gomerr.Configuration("Expected only one constraint type per field. Open a feature request ticket if this is needed.")
	}

	switch fieldConstraints[0][constraintTypeKey] {
	case UniqueConstraint:
		additionalFieldsString := fieldConstraints[0][additionalFieldsKey]
		var additionalFields []string
		if additionalFieldsString != "" {
			additionalFields = strings.Split(additionalFieldsString, ",")
		}
		return uniqueValueValidator{table: cvt.table, fieldName: structField.Name, additionalFields: additionalFields}, nil
	default:
		return nil, gomerr.Configuration("Unrecognized constraint type: " + fieldConstraints[0][constraintTypeKey])
	}
}

type uniqueValueValidator struct {
	table            *table
	fieldName        string
	additionalFields []string
}

func (u uniqueValueValidator) Apply(structValue reflect.Value, fieldValue reflect.Value, toolContext fields.ToolContext) gomerr.Gomerr {
	constrainedFields := toolContext[constrainedFieldsKey].(map[string]bool) // assumed to always be used in conjunction with collectConstrainedFieldsTool
	if !constrainedFields[u.fieldName] || fieldValue.IsZero() {
		return nil
	}

	newP := reflect.New(structValue.Type())
	newPElem := newP.Elem()
	newPElem.FieldByName(u.fieldName).Set(fieldValue)
	for _, additionalField := range u.additionalFields {
		newPElem.FieldByName(additionalField).Set(structValue.FieldByName(additionalField))
	}

	ge := u.table.Read(newP.Interface().(data.Persistable))
	if ge != nil {
		if _, ok := ge.Unwrap().(dataerr.PersistableNotFoundError); ok {
			return nil
		} else {
			return ge
		}
	}

	return constraint.NotSatisfiedBecause(map[string]interface{}{"Constraint": "Uniqueness"}, u.fieldName, structValue.Interface())
}

type attributeNameFieldTool struct {
}

func (a attributeNameFieldTool) Name() string {
	panic("implement me")
}

// "db.name"
func (a attributeNameFieldTool) New(structType reflect.Type, structField reflect.StructField, input interface{}) (fields.FieldToolApplier, gomerr.Gomerr) {
	if input == nil {
		return nil, nil
	}

	return attributeNameApplier{input.(string)}, nil
}

type attributeNameApplier struct {
	dbName string
}

func (a attributeNameApplier) Apply(structValue reflect.Value, fieldValue reflect.Value, toolContext fields.ToolContext) gomerr.Gomerr {
	// TODO: Handle "-"

	// TODO: both directions (i.e. handle dbName -> field; field -> dbName)
	/*
		func (pt *persistableType) dbNameToFieldName(dbName string) string {
			for k, v := range pt.dbNames {
				if v == dbName {
					return k
				}
			}

			return dbName // If we reach here, no alternative dbName was offered so must be the same as the field name
		}

	*/

	/*
		func (pt *persistableType) convertFieldNamesToDbNames(av *map[string]*dynamodb.AttributeValue) {
			cv := make(map[string]*dynamodb.AttributeValue, len(*av))
			for k, v := range *av {
				if dbName, ok := pt.dbNames[k]; ok {
					if dbName != "-" {
						cv[dbName] = v
					}
				} else {
					cv[k] = v
				}
			}

			*av = cv
		}

	*/
	panic("implement me")
}

func newKeyFieldTool(indexes map[string]*index) fields.FieldTool {
	return fields.RegexpWrapper{
		Regexp:       regexp.MustCompile(`(?:([\w-.]+):)?(pk|sk)(?:.(\d))?(?:=('\w+'))?`),
		RegexpGroups: []string{"", "index", "keyType", "keyPart", "staticValue"},
		FieldTool:    keyFieldTool{indexes},
	}
}

type keyFieldTool struct {
	indexes map[string]*index
}

func (k keyFieldTool) Name() string {
	panic("implement me")
}

// "db.keys"
func (k keyFieldTool) New(structType reflect.Type, structField reflect.StructField, input interface{}) (fields.FieldToolApplier, gomerr.Gomerr) {
	keyStatements := input.([]map[string]string)
	if len(keyStatements) == 0 {
		return nil, nil
	}

	for _, keyStatement := range keyStatements {
		idx, ok := k.indexes[keyStatement["index"]] // If no value, will default to the table itself
		if !ok {
			return nil, gomerr.Configuration("Undefined index: " + keyStatement["index"])
		}

		var key *keyAttribute
		if keyStatement["keyType"] == "pk" {
			key = idx.pk
		} else {
			key = idx.sk
		}

		var partIndex int // default to index 0
		if keyPart := keyStatement["keyPart"]; keyPart != "" {
			partIndex, _ = strconv.Atoi(keyPart)
		}

		var fieldName string
		if value := keyStatement["staticValue"]; value != "" { // If non-empty, this field has a static value. Replace with that value.
			fieldName = value
		} else {
			fieldName = structField.Name
		}

		stName := util.UnqualifiedTypeName(structType.Name())
		key.keyFieldsByPersistable[stName] = util.InsertStringAtIndex(key.keyFieldsByPersistable[stName], fieldName, partIndex)
	}

	// TODO:p1

	return nil, nil
}

type keyApplier struct {
	indexToPart map[string]int
	staticValue string
}

func (k keyApplier) Apply(structValue reflect.Value, fieldValue reflect.Value, toolContext fields.ToolContext) gomerr.Gomerr {
	// toolContext["<index_name>"] = []string
	// convert fieldValue to string

	panic("implement me")
}
