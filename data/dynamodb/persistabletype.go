package dynamodb

import (
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"unicode"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"

	"github.com/jt0/gomer/constraint"
	"github.com/jt0/gomer/data"
	"github.com/jt0/gomer/gomerr"
)

type persistableType struct {
	name             string
	dbNames          map[string]string                // field name -> storage name
	fieldConstraints map[string]constraint.Constraint // Map of field name -> constraint needed to be satisfied
	resolver         ItemResolver
}

func newPersistableType(table *table, persistableName string, pType reflect.Type) (*persistableType, gomerr.Gomerr) {
	pt := &persistableType{
		name:             persistableName,
		dbNames:          make(map[string]string, 0),
		fieldConstraints: make(map[string]constraint.Constraint, 0),
		resolver:         resolver(pType),
	}

	if errors := pt.processFields(pType, "", table, make([]gomerr.Gomerr, 0)); len(errors) > 0 {
		return nil, gomerr.Configuration("'db' tag errors found for type: " + persistableName).Wrap(gomerr.Batcher(errors))
	}

	return pt, nil
}

func resolver(pt reflect.Type) func(interface{}) (interface{}, gomerr.Gomerr) {
	return func(i interface{}) (interface{}, gomerr.Gomerr) {
		m, ok := i.(map[string]*dynamodb.AttributeValue)
		if !ok {
			return nil, gomerr.Internal("DynamoDB Item is not a map[string]*dynamodb.AttributeValue").AddAttribute("Actual", i)
		}

		resolved := reflect.New(pt).Interface().(data.Persistable)

		err := dynamodbattribute.UnmarshalMap(m, resolved)
		if err != nil {
			return nil, gomerr.Unmarshal(resolved.TypeName(), m, resolved).Wrap(err)
		}

		return resolved, nil
	}
}

func (pt *persistableType) processFields(structType reflect.Type, fieldPath string, table *table, errors []gomerr.Gomerr) []gomerr.Gomerr {
	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)
		fieldName := field.Name

		if field.Type.Kind() == reflect.Struct && field.Anonymous {
			errors = pt.processFields(field.Type, fieldPath+fieldName+".", table, errors)
		} else if unicode.IsLower([]rune(fieldName)[0]) {
			continue
		} else {
			pt.processNameTag(fieldName, field.Tag.Get("db.name"))

			errors = pt.processConstraintsTag(fieldName, field.Tag.Get("db.constraints"), table, errors)
			errors = pt.processKeysTag(fieldName, field.Tag.Get("db.keys"), table.indexes, errors)
		}
	}

	return errors
}

func (pt *persistableType) processNameTag(fieldName string, tag string) {
	if tag == "" {
		return
	}

	pt.dbNames[fieldName] = tag
}

var constraintsRegexp = regexp.MustCompile(`(unique)(\(([\w,]+)\))?`)

func (pt *persistableType) processConstraintsTag(fieldName string, tag string, t *table, errors []gomerr.Gomerr) []gomerr.Gomerr {
	if tag == "" {
		return errors
	}

	constraints := constraintsRegexp.FindAllStringSubmatch(tag, -1)
	if constraints == nil {
		return append(errors, gomerr.Configuration("Invalid `db.constraints` value: "+tag).AddAttribute("Field", fieldName))
	}

	for _, c := range constraints {
		switch c[1] {
		case "unique":
			var additionalFields []string
			if c[3] != "" {
				additionalFields = strings.Split(c[3], ",")
			}
			pt.fieldConstraints[fieldName] = constraint.NewType(t.isFieldUnique(fieldName, additionalFields), "Validate", "Field Uniqueness", "Field", fieldName, "Scope", additionalFields)
		}
	}

	return errors
}

var ddbKeyStatementRegexp = regexp.MustCompile(`(?:(!)?(\+|-)?([\w-.]+)?:)?(pk|sk)(?:.(\d))?(?:=('\w+')(\+)?)?`)

func (pt *persistableType) processKeysTag(fieldName string, tag string, indexes map[string]*index, errors []gomerr.Gomerr) []gomerr.Gomerr {
	if tag == "" {
		return nil
	}

	for _, keyStatement := range strings.Split(strings.ReplaceAll(tag, " ", ""), ",") {
		groups := ddbKeyStatementRegexp.FindStringSubmatch(keyStatement)
		if groups == nil {
			return append(errors, gomerr.Configuration("Invalid `db.keys` value: "+keyStatement).AddAttribute("Field", fieldName))
		}

		idx, ok := indexes[groups[3]]
		if !ok {
			return append(errors, gomerr.Configuration(fmt.Sprintf("Undefined index: %s", groups[3])).AddAttribute("Field", fieldName))
		}

		var key *keyAttribute
		if groups[4] == "pk" {
			key = idx.pk
		} else {
			key = idx.sk
		}

		var partIndex int // default to index 0
		if groups[5] != "" {
			partIndex, _ = strconv.Atoi(groups[5])
		}

		if groups[6] != "" { // If non-empty, this field has a static value. Replace with that value.
			fieldName = groups[6]
		}

		// TODO: Determine scenarios where skLength/skMissing don't map to desired behavior. May need preferred
		//       priority levels to compensate
		kf := keyField{name: fieldName, preferred: groups[1] == "!", ascending: groups[2] != "-"}
		key.keyFieldsByPersistable[pt.name] = insertAtIndex(key.keyFieldsByPersistable[pt.name], &kf, partIndex)
	}

	return errors
}

func insertAtIndex(slice []*keyField, value *keyField, index int) []*keyField {
	if slice == nil || cap(slice) == 0 {
		slice = make([]*keyField, 0, index+1)
	}

	lenKeyFields := len(slice)
	capKeyFields := cap(slice)
	if index < lenKeyFields {
		if slice[index] != nil {
			panic(fmt.Sprintf("already found value '%v' at index %d", slice[index], index))
		}
	} else if index < capKeyFields {
		slice = slice[0 : index+1]
	} else {
		slice = append(slice, make([]*keyField, index+1-capKeyFields)...)
	}

	slice[index] = value

	return slice
}

func (pt *persistableType) dbNameToFieldName(dbName string) string {
	for k, v := range pt.dbNames {
		if v == dbName {
			return k
		}
	}

	return dbName // If we reach here, no alternative dbName was offered so must be the same as the field name
}

func (pt *persistableType) convertFieldNamesToDbNames(av *map[string]*dynamodb.AttributeValue) {
	if len(pt.dbNames) == 0 {
		return
	}

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
