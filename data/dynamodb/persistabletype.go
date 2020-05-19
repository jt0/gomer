package dynamodb

import (
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"unicode"

	"github.com/aws/aws-sdk-go/service/dynamodb"

	"github.com/jt0/gomer/util"
)

type persistableType struct {
	name         string
	dbNames      map[string]string   // field name -> storage name
	uniqueFields map[string][]string // Map of field name -> set of fields that determine uniqueness
}

func NewPersistableType(persistableName string, pType reflect.Type, indexes map[string]*index) *persistableType {
	pt := &persistableType{
		name:         persistableName,
		uniqueFields: make(map[string][]string, 0),
		dbNames:      make(map[string]string),
	}

	pt.processFields(pType, "", indexes)

	return pt
}

func (pt *persistableType) processFields(structType reflect.Type, fieldPath string, indexes map[string]*index) {
	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)
		fieldName := field.Name

		if field.Type.Kind() == reflect.Struct && field.Anonymous {
			pt.processFields(field.Type, fieldPath+fieldName+".", indexes)
		} else if unicode.IsLower([]rune(fieldName)[0]) {
			continue
		} else {
			pt.processNameTag(fieldName, field.Tag.Get("db.name"))
			pt.processConstraintsTag(fieldName, field.Tag.Get("db.constraints"))
			pt.processKeysTag(fieldName, field.Tag.Get("db.keys"), indexes)
		}
	}
}

func (pt *persistableType) processNameTag(fieldName string, tag string) {
	if tag == "" {
		return
	}

	pt.dbNames[fieldName] = tag
}

var constraintsRegex = regexp.MustCompile(`(\w+)(\(([\w,]+)\))?`)

func (pt *persistableType) processConstraintsTag(fieldName string, tag string) {
	if tag == "" {
		return
	}

	constraints := constraintsRegex.FindAllStringSubmatch(tag, -1)
	if constraints == nil {
		panic("Improperly formatted db.constraints element: " + tag)
	}

	for _, constraint := range constraints {
		switch constraint[1] {
		case "unique":
			if constraint[3] == "" {
				pt.uniqueFields[fieldName] = nil
			} else {
				pt.uniqueFields[fieldName] = strings.Split(constraint[3], ",")
			}
		default:
			panic("Unknown constraint type: " + constraint[1])
		}
	}
}

var ddbKeyStatementRegex = regexp.MustCompile(`(([\w-.]+):)?(pk|sk)(.(\d))?(=('\w+'))?`)

func (pt *persistableType) processKeysTag(fieldName string, tag string, indexes map[string]*index) {
	if tag == "" {
		return
	}

	for _, keyStatement := range strings.Split(strings.ReplaceAll(tag, " ", ""), ",") {
		groups := ddbKeyStatementRegex.FindStringSubmatch(keyStatement)
		if groups == nil {
			panic("Improperly formatted db.keys element: " + keyStatement)
		}

		index, ok := indexes[groups[2]]
		if !ok {
			panic("unknown index '" + groups[2] + "'")
		}

		var key *keyAttribute
		if groups[3] == "pk" {
			key = index.pk
		} else {
			key = index.sk
		}

		var partIndex int // default to index 0
		if groups[5] != "" {
			i, err := strconv.Atoi(groups[5])
			if err != nil || i < 0 || i > 9 {
				panic("invalid index value (number between 0 and 9) for db.keys element: " + keyStatement)
			}
			partIndex = i
		}

		if groups[7] != "" { // If non-empty, this field has a static value. Replace with that value.
			fieldName = groups[7]
		}

		key.keyFieldsByPersistable[pt.name] = util.InsertStringAtIndex(key.keyFieldsByPersistable[pt.name], fieldName, partIndex)
	}
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
