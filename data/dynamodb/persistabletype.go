package dynamodb

import (
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"unicode"

	"github.com/aws/aws-sdk-go/service/dynamodb"

	"github.com/jt0/gomer/constraint"
	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/util"
)

type persistableType struct {
	name         string
	dbNames      map[string]string   // field name -> storage name
	uniqueFields map[string][]string // Map of field name -> set of fields that determine uniqueness
}

func newPersistableType(persistableName string, pType reflect.Type, indexes map[string]*index) (*persistableType, gomerr.Gomerr) {
	pt := &persistableType{
		name:         persistableName,
		dbNames:      make(map[string]string, 0),
		uniqueFields: make(map[string][]string, 0),
	}

	if errors := pt.processFields(pType, "", indexes, make([]gomerr.Gomerr, 0)); len(errors) > 0 {
		return nil, gomerr.Batch(errors).AddNotes("'db' tag errors found for type: " + persistableName).AddCulprit(gomerr.Configuration)
	}

	return pt, nil
}

func (pt *persistableType) processFields(structType reflect.Type, fieldPath string, indexes map[string]*index, errors []gomerr.Gomerr) []gomerr.Gomerr {
	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)
		fieldName := field.Name

		if field.Type.Kind() == reflect.Struct && field.Anonymous {
			errors = pt.processFields(field.Type, fieldPath+fieldName+".", indexes, errors)
		} else if unicode.IsLower([]rune(fieldName)[0]) {
			continue
		} else {
			pt.processNameTag(fieldName, field.Tag.Get("db.name"))

			errors = pt.processConstraintsTag(fieldName, field.Tag.Get("db.constraints"), errors)
			errors = pt.processKeysTag(fieldName, field.Tag.Get("db.keys"), indexes, errors)
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

var constraintsRegex = regexp.MustCompile(`(unique)(\(([\w,]+)\))?`)

func (pt *persistableType) processConstraintsTag(fieldName string, tag string, errors []gomerr.Gomerr) []gomerr.Gomerr {
	if tag == "" {
		return errors
	}

	constraints := constraintsRegex.FindAllStringSubmatch(tag, -1)
	if constraints == nil {
		return append(errors, gomerr.BadValue("'constraints' tag", tag, constraint.Regexp(constraintsRegex)).AddNotes("field: "+fieldName))
	}

	for _, c := range constraints {
		switch c[1] {
		case "unique":
			if c[3] == "" {
				pt.uniqueFields[fieldName] = nil
			} else {
				pt.uniqueFields[fieldName] = strings.Split(c[3], ",")
			}
		}
	}

	return errors
}

var ddbKeyStatementRegex = regexp.MustCompile(`(([\w-.]+):)?(pk|sk)(.(\d))?(=('\w+'))?`)

func (pt *persistableType) processKeysTag(fieldName string, tag string, indexes map[string]*index, errors []gomerr.Gomerr) []gomerr.Gomerr {
	if tag == "" {
		return nil
	}

	for _, keyStatement := range strings.Split(strings.ReplaceAll(tag, " ", ""), ",") {
		groups := ddbKeyStatementRegex.FindStringSubmatch(keyStatement)
		if groups == nil {
			return append(errors, gomerr.BadValue("'ddbKey' tag", tag, constraint.Regexp(ddbKeyStatementRegex)).AddNotes("field: "+fieldName))
		}

		index, ok := indexes[groups[2]]
		if !ok {
			return append(errors, gomerr.NotFound("ddb.Index", groups[2]).AddNotes("field: "+fieldName).AddCulprit(gomerr.Configuration))
		}

		var key *keyAttribute
		if groups[3] == "pk" {
			key = index.pk
		} else {
			key = index.sk
		}

		var partIndex int // default to index 0
		if groups[5] != "" {
			partIndex, _ = strconv.Atoi(groups[5])
		}

		if groups[7] != "" { // If non-empty, this field has a static value. Replace with that value.
			fieldName = groups[7]
		}

		key.keyFieldsByPersistable[pt.name] = util.InsertStringAtIndex(key.keyFieldsByPersistable[pt.name], fieldName, partIndex)
	}

	return errors
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
